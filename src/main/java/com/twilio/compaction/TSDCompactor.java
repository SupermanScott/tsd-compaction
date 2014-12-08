package com.twilio.compaction;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.PriorityQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.regionserver.compactions.DefaultCompactor;
import org.apache.hadoop.hbase.regionserver.compactions.Compactor.CellSink;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Bytes;

public class TSDCompactor extends DefaultCompactor {
    private static final Log LOG = LogFactory.getLog(TSDCompactor.class);

    /** Flag to determine if a compacted column is a mix of seconds and ms */
    private static final byte MS_MIXED_COMPACT = 1;

    private int compactionKVMax;

    public TSDCompactor(final Configuration conf, final Store store) {
        super(conf, store);

        this.compactionKVMax =
            this.conf.getInt(HConstants.COMPACTION_KV_MAX, HConstants.COMPACTION_KV_MAX_DEFAULT);
    }

    /**
     * Performs the compaction according to OpenTSDB rules.
     * @param scanner Where to read from.
     * @param writer Where to write to.
     * @param smallestReadPoint Smallest read point.
     * @return Whether compaction ended; false if it was interrupted for some reason.
     */
    protected boolean performCompaction(InternalScanner scanner,
                                        CellSink writer, long smallestReadPoint) throws IOException {
        long bytesWritten = 0;
        long bytesWrittenProgress = 0;
        // Since scanner.next() can return 'false' but still be delivering data,
        // we have to use a do/while loop.
        List<Cell> kvs = new ArrayList<Cell>();
        long closeCheckInterval = HStore.getCloseCheckInterval();
        long lastMillis = 0;
        if (LOG.isDebugEnabled()) {
            lastMillis = EnvironmentEdgeManager.currentTimeMillis();
        }
        final long now = EnvironmentEdgeManager.currentTimeMillis();
        boolean hasMore;
        final long cutOff = now / 1000 - 3600 - 1;
        do {
            hasMore = scanner.next(kvs, compactionKVMax);
            // To figure out if row is a one to TSD compact, getUnsignedInt
            // with offset of 3
            // https://github.com/OpenTSDB/asynchbase/blob/master/src/Bytes.java#L151-L178
            final byte[] b = KeyValueUtil.ensureKeyValue(kvs.get(0)).getKey();
            final long baseTime = Internal.getUnsignedInt(b, 3);
            final boolean compactRow = baseTime > cutOff && kvs.size() > 1;

            if (compactRow) {
                int len = writeCompactedRow(kvs, writer, smallestReadPoint);
                if (closeCheckInterval > 0) {
                    bytesWritten += len;
                    if (bytesWritten > closeCheckInterval) {
                        bytesWritten = 0;
                        if (!store.areWritesEnabled()) {
                            progress.cancel();
                            return false;
                        }
                    }
                }
                continue;
            }
            // Default compactor
            for (Cell c : kvs) {
                KeyValue kv = KeyValueUtil.ensureKeyValue(c);
                if (kv.getMvccVersion() <= smallestReadPoint) {
                    kv.setMvccVersion(0);
                }
                writer.append(kv);
                int len = kv.getLength();
                ++progress.currentCompactedKVs;
                progress.totalCompactedSize += len;
                if (LOG.isDebugEnabled()) {
                    bytesWrittenProgress += len;
                }

                // check periodically to see if a system stop is requested
                if (closeCheckInterval > 0) {
                    bytesWritten += len;
                    if (bytesWritten > closeCheckInterval) {
                        bytesWritten = 0;
                        if (!store.areWritesEnabled()) {
                            progress.cancel();
                            return false;
                        }
                    }
                }
            }
            // Log the progress of long running compactions every minute if
            // logging at DEBUG level
            if (LOG.isDebugEnabled()) {
                if ((now - lastMillis) >= 60 * 1000) {
                    LOG.debug("Compaction progress: " + progress + String.format(", rate=%.2f kB/sec",
                                                                                 (bytesWrittenProgress / 1024.0) / ((now - lastMillis) / 1000.0)));
                    lastMillis = now;
                    bytesWrittenProgress = 0;
                }
            }
            kvs.clear();
        } while (hasMore);
        progress.complete();
        return true;
    }

    protected int writeCompactedRow(List<Cell> kvs, CellSink writer, long smallestReadPoint) throws IOException {
        final int totalValues = kvs.size();
        final ByteBufferList compactedQualifier = new ByteBufferList(totalValues);
        final ByteBufferList compactedValue = new ByteBufferList(totalValues);

        PriorityQueue<ColumnDatapointIterator> heap = new PriorityQueue<ColumnDatapointIterator>(totalValues);
        boolean first = true;
        KeyValue firstKv = KeyValueUtil.ensureKeyValue(kvs.get(0));
        final byte[] rowKey = new byte[firstKv.getRowLength()];
        Bytes.putBytes(rowKey, 0, firstKv.getRowArray(),
                       firstKv.getRowOffset(), firstKv.getRowLength());
        final byte[] family = new byte[firstKv.getFamilyLength()];
        Bytes.putBytes(family, 0, firstKv.getFamilyArray(),
                       firstKv.getFamilyOffset(), firstKv.getFamilyLength());
        int totalLen = 0;
        long maxMvcc = 0;
        for (Cell c: kvs) {
            final KeyValue kv = KeyValueUtil.ensureKeyValue(c);
            heap.add(new ColumnDatapointIterator(kv.getTimestamp(),
                                                 c.getQualifierArray(),
                                                 c.getValueArray()));

            totalLen += KeyValueUtil.length(kv);
            if (kv.getMvccVersion() > maxMvcc &&
                kv.getMvccVersion() > smallestReadPoint) {
                maxMvcc = kv.getMvccVersion();
            }
        }

        final boolean mixedCompact = mergeDatapoints(compactedQualifier, compactedValue, heap);
        KeyValue compactedKv = buildCompactedColumn(compactedQualifier, compactedValue,
                                                    mixedCompact, rowKey, family);
        if (maxMvcc > smallestReadPoint) {
            compactedKv.setMvccVersion(maxMvcc);
        }

        writer.append(compactedKv);
        return totalLen;
    }

    /**
     * Process datapoints from the heap in order, merging into a sorted list.  Handles duplicates
     * by keeping the most recent (based on HBase column timestamps; if duplicates in the )
     *
     * @param compacted_qual qualifiers for sorted datapoints
     * @param compacted_val values for sorted datapoints
     * @param heap Heap of ColumnDatapointIterators
     */
    private boolean mergeDatapoints(ByteBufferList compacted_qual, ByteBufferList compacted_val, PriorityQueue<ColumnDatapointIterator> heap) {
      int prevTs = -1;
      boolean ms_in_row = false;
      boolean s_in_row = false;

      while (!heap.isEmpty()) {
        final ColumnDatapointIterator col = heap.remove();
        final int ts = col.getTimestampOffsetMs();
        if (ts == prevTs) {
          // check to see if it is a complete duplicate, or if the value changed
          final byte[] existingVal = compacted_val.getLastSegment();
          final byte[] discardedVal = col.getCopyOfCurrentValue();
          if (!Arrays.equals(existingVal, discardedVal)) {
            LOG.warn("Duplicate timestamp"
                + ", ms_offset=" + ts + ", kept=" + Arrays.toString(existingVal) + ", discarded="
                + Arrays.toString(discardedVal));
          }
        }
        else {
          prevTs = ts;
          col.writeToBuffers(compacted_qual, compacted_val);
          ms_in_row |= col.isMilliseconds();
          s_in_row |= !col.isMilliseconds();
        }
        if (col.advance()) {
          // there is still more data in this column, so add it back to the heap
          heap.add(col);
        }
      }
      return ms_in_row && s_in_row;
    }

    /**
     * Build the compacted column from the list of byte buffers that were
     * merged together.
     *
     * @param compacted_qual list of merged qualifiers
     * @param compacted_val list of merged values
     * @param mixedCompact Whether or not this is column contains seconds and milliseconds
     * @param rowKey The key of the row
     @ param family the column family for the new KeyValue
     *
     * @return {@link KeyValue} instance for the compacted column
     */
    private KeyValue buildCompactedColumn(final ByteBufferList compacted_qual,
                                          final ByteBufferList compacted_val,
                                          final boolean mixedCompact,
                                          final byte[] rowKey,
                                          final byte[] family) {
      // metadata is a single byte for a multi-value column, otherwise nothing
      final int metadata_length = compacted_val.segmentCount() > 1 ? 1 : 0;
      final byte[] cq = compacted_qual.toBytes(0);
      final byte[] cv = compacted_val.toBytes(metadata_length);

      // add the metadata flag, which right now only includes whether we mix s/ms datapoints
      if (metadata_length > 0) {
        byte metadata_flag = 0;
        if (mixedCompact) {
          metadata_flag |= MS_MIXED_COMPACT;
        }
        cv[cv.length - 1] = metadata_flag;
      }

      return new KeyValue(rowKey, family, cq, cv);
    }

}
