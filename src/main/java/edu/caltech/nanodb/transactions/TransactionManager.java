package edu.caltech.nanodb.transactions;


import edu.caltech.nanodb.server.NanoDBServer;
import edu.caltech.nanodb.server.SessionState;
import edu.caltech.nanodb.storage.*;
import edu.caltech.nanodb.storage.writeahead.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * The Transaction Manager is responsible for managing all aspects of proper
 * transaction processing within the database.  It works closely with the
 * other components of the Storage Manager to make this happen correctly.
 */
public class TransactionManager implements BufferManagerObserver {
    /**
     * A logging object for reporting anything interesting that happens.
     */
    private static Logger logger = LogManager.getLogger(TransactionManager.class);


    /**
     * This is the name of the file that the Transaction Manager uses to keep
     * track of overall transaction state.
     */
    private static final String TXNSTATE_FILENAME = "txnstate.dat";


    /**
     * A reference to the server that this transaction manager is operating
     * within.
     */
    private NanoDBServer server;


    /**
     * A reference to the storage manager that this transaction manager is
     * operating within.
     */
    private StorageManager storageManager;


    /**
     * The write-ahead logger that records transaction details.
     */
    private WALManager walManager;


    /**
     * This variable keeps track of the next transaction ID that should be used
     * for a transaction.  It is initialized when the transaction manager is
     * started.
     */
    private AtomicInteger nextTxnID;


    /**
     * This is the last value of nextLSN saved to the transaction-state file.
     */
    private LogSequenceNumber txnStateNextLSN;


    public TransactionManager(NanoDBServer server) {

        this.server = server;
        this.storageManager = server.getStorageManager();

        BufferManager bufferManager = storageManager.getBufferManager();

        // Register the transaction manager on the buffer manager so that we
        // can enforce the write-ahead logging rule for evicted pages.
        bufferManager.addObserver(this);

        this.nextTxnID = new AtomicInteger();

        walManager = new WALManager(storageManager);
    }


    /**
     * This helper function initializes a brand new transaction-state file for
     * the transaction manager to use for providing transaction atomicity and
     * durability.
     *
     * @return a {@code DBFile} object for the newly created and initialized
     * transaction-state file.
     */
    private TransactionStatePage createTxnStateFile() {
        // Create a brand new transaction-state file for the Transaction Manager
        // to use.

        DBFile dbfTxnState;
        dbfTxnState = storageManager.createDBFile(TXNSTATE_FILENAME,
            DBFileType.TXNSTATE_FILE);

        DBPage dbpTxnState = storageManager.loadDBPage(dbfTxnState, 0);
        TransactionStatePage txnState = new TransactionStatePage(dbpTxnState);

        // Set the "next transaction ID" value to an initial default.
        txnState.setNextTransactionID(1);
        nextTxnID.set(1);

        // Set the "first LSN" and "next LSN values to initial defaults.
        LogSequenceNumber lsn =
            new LogSequenceNumber(0, WALManager.OFFSET_FIRST_RECORD);

        txnState.setFirstLSN(lsn);
        txnState.setNextLSN(lsn);
        txnStateNextLSN = lsn;

        storageManager.getBufferManager().writeDBFile(dbfTxnState, /* sync */ true);

        return txnState;
    }


    private TransactionStatePage loadTxnStateFile() {
        DBFile dbfTxnState = storageManager.openDBFile(TXNSTATE_FILENAME);
        if (dbfTxnState == null)
            return null;

        DBPage dbpTxnState = storageManager.loadDBPage(dbfTxnState, 0);
        if (dbpTxnState == null)
            return null;


        TransactionStatePage txnState = new TransactionStatePage(dbpTxnState);

        // Set the "next transaction ID" value properly.
        nextTxnID.set(txnState.getNextTransactionID());

        // Retrieve the "first LSN" and "next LSN values so we know the range of
        // the write-ahead log that we need to apply for recovery.
        txnStateNextLSN = txnState.getNextLSN();

        return txnState;
    }


    private void storeTxnStateToFile() {
        DBFile dbfTxnState = storageManager.openDBFile(TXNSTATE_FILENAME);
        DBPage dbpTxnState = storageManager.loadDBPage(dbfTxnState, 0);
        TransactionStatePage txnState = new TransactionStatePage(dbpTxnState);

        txnState.setNextTransactionID(nextTxnID.get());
        txnState.setFirstLSN(walManager.getFirstLSN());
        txnState.setNextLSN(txnStateNextLSN);

        storageManager.getBufferManager().writeDBFile(dbfTxnState, /* sync */ true);
    }


    public void initialize() {
        // Read the transaction-state file so we can initialize the
        // Transaction Manager.

        TransactionStatePage txnState = loadTxnStateFile();
        if (txnState == null) {
            // BUGBUG:  If we find any other files in the data directory, we
            //          really should fail initialization, because the old files
            //          may have been created without transaction processing...

            logger.info("Couldn't find transaction-state file " +
                TXNSTATE_FILENAME + ", creating.");

            txnState = createTxnStateFile();
        }

        // Perform recovery, and get the new "first LSN" value

        LogSequenceNumber firstLSN = txnState.getFirstLSN();
        LogSequenceNumber nextLSN = txnState.getNextLSN();
        logger.debug(String.format("Txn State has FirstLSN = %s, NextLSN = %s",
            firstLSN, nextLSN));

        RecoveryInfo recoveryInfo = walManager.doRecovery(firstLSN, nextLSN);

        // Set the "next transaction ID" value based on what recovery found
        int recNextTxnID = recoveryInfo.maxTransactionID + 1;
        if (recNextTxnID != -1 && recNextTxnID > nextTxnID.get()) {
            logger.info("Advancing NextTransactionID from " +
                nextTxnID.get() + " to " + recNextTxnID);
            nextTxnID.set(recNextTxnID);
        }

        // Update and sync the transaction state if any changes were made.
        storeTxnStateToFile();

        // Register the component that manages indexes when tables are modified.
        server.getEventDispatcher().addCommandEventListener(
            new TransactionStateUpdater(this));
    }


    /**
     * Returns the "next transaction ID" value without incrementing it.
     * This operation is thread-safe.
     *
     * @return the next transaction ID to use
     */
    private int getNextTxnID() {
        return nextTxnID.get();
    }


    /**
     * Returns the "next transaction ID" value, and also increments this
     * value.  This operation is thread-safe.
     *
     * @return the next transaction ID to use
     */
    private int getAndIncrementNextTxnID() {
        return nextTxnID.getAndIncrement();
    }


    public void startTransaction(boolean userStarted)
        throws TransactionException {

        SessionState state = SessionState.get();
        TransactionState txnState = state.getTransactionState();

        if (txnState.isTxnInProgress())
            throw new IllegalStateException("A transaction is already in progress!");

        int txnID = getAndIncrementNextTxnID();
        txnState.setTransactionID(txnID);
        txnState.setUserStartedTxn(userStarted);

        logger.debug("Starting transaction with ID " + txnID +
            (userStarted ? " (user-started)" : ""));

        // Don't record a "start transaction" WAL record until the transaction
        // actually writes to something in the database.
    }


    public void recordPageUpdate(DBPage dbPage) {
        if (!dbPage.isDirty()) {
            logger.debug("Page reports it is not dirty; not logging update.");
            return;
        }

        logger.debug("Recording page-update for page " + dbPage.getPageNo() +
            " of file " + dbPage.getDBFile());

        TransactionState txnState = SessionState.get().getTransactionState();
        if (!txnState.hasLoggedTxnStart()) {
            walManager.writeTxnRecord(WALRecordType.START_TXN);
            txnState.setLoggedTxnStart(true);
        }

        walManager.writeUpdatePageRecord(dbPage);
        dbPage.syncOldPageData();
    }


    public void commitTransaction() throws TransactionException {
        SessionState state = SessionState.get();
        TransactionState txnState = state.getTransactionState();

        if (!txnState.isTxnInProgress()) {
            // The user issued a COMMIT without starting a transaction!

            state.getOutputStream().println(
                "No transaction is currently in progress.");

            return;
        }

        int txnID = txnState.getTransactionID();

        if (txnState.hasLoggedTxnStart()) {
            // Must record the transaction as committed to the write-ahead log.
            // Then, we must force the WAL to include this commit record.
            walManager.writeTxnRecord(WALRecordType.COMMIT_TXN);
            forceWAL(walManager.getNextLSN());
        } else {
            logger.debug("Transaction " + txnID + " has made no changes; not " +
                "recording transaction-commit to WAL.");
        }

        // Now that the transaction is successfully committed, clear the current
        // transaction state.
        logger.debug("Transaction completed, resetting transaction state.");
        txnState.clear();
    }


    public void rollbackTransaction() throws TransactionException, WALFileException {
        SessionState state = SessionState.get();
        TransactionState txnState = state.getTransactionState();

        if (!txnState.isTxnInProgress()) {
            // The user issued a ROLLBACK without starting a transaction!

            state.getOutputStream().println(
                "No transaction is currently in progress.");

            return;
        }

        int txnID = txnState.getTransactionID();

        if (txnState.hasLoggedTxnStart()) {
            // Must rollback the transaction using the write-ahead log.
            walManager.rollbackTransaction();
        } else {
            logger.debug("Transaction " + txnID + " has made no changes; not " +
                "recording transaction-rollback to WAL.");
        }

        // Now that the transaction is successfully rolled back, clear the
        // current transaction state.
        logger.debug("Transaction completed, resetting transaction state.");
        txnState.clear();
    }


    /**
     * This method is registered on the {@link BufferManager}, to ensure that
     * the write-ahead logging rule is enforced.  Specifically, all dirty
     * pages to be evicted from the buffer manager must be reflected in the
     * write-ahead log on disk, before they are evicted.
     *
     * @param pages the collection of pages that are about to be evicted.
     */
    @Override
    public void beforeWriteDirtyPages(List<DBPage> pages) {
        logger.debug("Write WAL before writing dirty pages");
        LogSequenceNumber maxLSN = null;

        for (DBPage pg : pages) {
            var dbType = pg.getDBFile().getType();
            if (dbType != DBFileType.WRITE_AHEAD_LOG_FILE && dbType != DBFileType.TXNSTATE_FILE) {
                // Get page's LSN
                var lsn = pg.getPageLSN();
                if (lsn == null)
                    logger.debug("Page[" + pg.getPageNo() + "] has no lsn");

                maxLSN = maxLSN == null || maxLSN.compareTo(lsn) < 0 ? lsn : maxLSN;
            }
        }
        forceWAL(maxLSN);
    }


    /**
     * This method forces the write-ahead log out to at least the specified
     * log sequence number, syncing the log to ensure that all essential
     * records have reached the disk itself.
     * <p>
     * Atomicity: Write LSNs first, update nextLSN second. The redundant LSNs
     * do not matter.
     * <p>
     * Duration: Write pages to disk.
     *
     * @param lsn All WAL data up to this value must be forced to disk and
     *            sync'd.  This value may be one past the end of the current WAL
     *            file during normal operation.
     */
    public void forceWAL(LogSequenceNumber lsn) {
        // 1. check whether we need to sync
        if (lsn == null || txnStateNextLSN.compareTo(lsn) >= 0) {
            return;
        }

        // 2. write part of buffered-logs to files
        DBFile file;
        int fileNo;
        var bpm = storageManager.getBufferManager();

        int lastPosition = lsn.getFileOffset() + lsn.getRecordSize();
        var newNext = WALManager.computeNextLSN(lsn.getLogFileNo(), lastPosition);
        var oldNext = txnStateNextLSN;

        if (oldNext.getLogFileNo() == newNext.getLogFileNo()) {
            // the buffered-logs all in the same file
            fileNo = oldNext.getLogFileNo();
            file = bpm.getFile(WALManager.getWALFileName(oldNext.getLogFileNo()));
            writePages(fileNo,
                (oldNext.getFileOffset() - 1) / file.getPageSize(),
                (newNext.getFileOffset() - 1) / file.getPageSize());
        } else {
            // write to start page
            fileNo = oldNext.getLogFileNo();
            file = bpm.getFile(WALManager.getWALFileName(fileNo));
            writePages(fileNo,
                (oldNext.getFileOffset() - 1) / file.getPageSize(),
                file.getNumPages() - 1);

            // write (start, end) pages
            for (fileNo = oldNext.getLogFileNo() + 1; fileNo < newNext.getLogFileNo(); fileNo++) {
                file = bpm.getFile(WALManager.getWALFileName(fileNo));
                writePages(fileNo, 0, file.getNumPages() - 1);
            }

            // write end page
            fileNo = newNext.getLogFileNo();
            file = bpm.getFile(WALManager.getWALFileName(fileNo));
            writePages(fileNo, 0, (newNext.getFileOffset() - 1) / file.getPageSize());
        }

        txnStateNextLSN = newNext;
        storeTxnStateToFile();
    }

    private void writePages(int fileNo, int startPgNo, int endPgNo) {
        var bpm = storageManager.getBufferManager();
        var file = bpm.getFile(WALManager.getWALFileName(fileNo));
        if (file != null) {
            bpm.writeDBFile(file, startPgNo, endPgNo, true);
            logger.debug(String.format("Syncing files[%d], pages [%d, %d]", fileNo, startPgNo, endPgNo));
        }
    }


    /**
     * This method forces the entire write-ahead log out to disk, syncing the
     * log as well.  This version is intended to be used during shutdown
     * processing in order to record all WAL changes to disk.
     *
     * @throws IOException if an IO error occurs while attempting to force the
     *                     WAL file to disk.  If a failure occurs, the database is probably
     *                     going to be broken.
     */
    public void forceWAL() {
        forceWAL(walManager.getNextLSN());
    }
}
