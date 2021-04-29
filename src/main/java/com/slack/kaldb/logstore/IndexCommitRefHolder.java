package com.slack.kaldb.logstore;

import org.apache.lucene.index.IndexCommit;

import java.io.Closeable;
import java.io.IOException;

/**
 * This class takes a snapshot of the current index usually by calling SnapshotDeletionPolicy#snapshot and
 * a runnable which should close the underlying resource ( call SnapshotDeletionPolicy#release )
 *
 */
public class IndexCommitRefHolder implements Closeable {
  private final CheckedRunnable<IOException> onClose;
  private final IndexCommit indexCommit;

  public IndexCommitRefHolder(IndexCommit indexCommit, CheckedRunnable<IOException> onClose) {
    this.indexCommit = indexCommit;
    this.onClose = onClose;
  }

  @Override
  public void close() throws IOException {
    onClose.run();
  }

  public IndexCommit getIndexCommit() {
    return indexCommit;
  }
}
