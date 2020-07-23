package de.mpc.pia.intermediate.compiler;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import uk.ac.ebi.jmzidml.model.mzidml.SpectrumIdentification;
import de.mpc.pia.intermediate.Accession;
import de.mpc.pia.intermediate.PIAInputFile;
import de.mpc.pia.intermediate.Peptide;
import de.mpc.pia.intermediate.PeptideSpectrumMatch;

/**
 * This class is used to read in one or several input files and compile them
 * into one PIA SQLite intermediate file.
 *
 * @author julian
 *
 */
public class PIASQLiteCompiler extends PIACompiler {

    /** file name of the SQLite database*/
    private String databaseFileName;

    /** connection to the database */
    private Connection connection;


    /** SQL for inserting new accession */
    private static String sqlNewAccession = "INSERT INTO accessions(accession,sequence) VALUES(?,?)";

    /** SQL to get the accession ID */
    private static String sqlGetAccessionID = "SELECT id FROM accessions WHERE accession = ?";



    /** map of the accessions, maps from the accession id to the Accession */
    private Map<Long, Accession> accessions;

    /** map of peptides, maps from the Id to the peptides */
    private Map<Long, Peptide> peptides;

    /** map of peptides, maps from the sequence to the peptides */
    private Map<String, Long> peptideSequencesToIDs;

    /** map of spectra, maps from the IDs to the PSMs */
    private Map<Long, PeptideSpectrumMatch> spectra;

    /** maps from the accession IDs to the peptide IDs, used to calculate clusters*/
    private Map<Long, Set<Long>> accPepMapIDs;

    /** maps from the peptide to the accessions, used to calculate the clusters */
    private Map<Long, Set<Long>> pepAccMapIDs;


    /** logger for this class */
    private static final Logger LOGGER = Logger.getLogger(PIASQLiteCompiler.class);

    /**
     * Basic constructor
     * @throws SQLException
     */
    public PIASQLiteCompiler(String fileName) throws SQLException {
        super();

        databaseFileName = fileName;
        connection = connect();

        if (!createTables()) {
            throw new SQLException("Could not connect to SQLite or create tables");
        }

        accessions = new HashMap<>();
        peptides = new HashMap<>();
        peptideSequencesToIDs = new HashMap<>();
        spectra = new HashMap<>();

        accPepMapIDs = new HashMap<>();
        pepAccMapIDs = new HashMap<>();
    }


    /**
     * Connect to the database file / create it
     *
     * @return the Connection object (null if not connected)
     */
    private Connection connect() {
        // SQLite connection string
        String url = "jdbc:sqlite:" + databaseFileName;
        Connection conn = null;

        try {
            // check, whether file exists (and delete, if it does)
            File f = new File(databaseFileName);
            if (f.exists() && !f.isDirectory()) {
                LOGGER.info(databaseFileName + "exists, deleting old file");
                f.delete();
            }

            conn = DriverManager.getConnection(url);
            conn.setAutoCommit(false);
        } catch (Exception e) {
            LOGGER.error(e);
            conn = null;
        }

        return conn;
    }


    /**
     * Creates the tables in the database
     *
     * @return
     */
    private boolean createTables() {
        if(connection == null) {
            return false;
        }

        boolean result;
        try (Statement stmt = connection.createStatement()) {
            // create the tables
            stmt.execute("CREATE TABLE groups ("
                    + "     id integer PRIMARY KEY,"
                    + "     treeID integer"
//                    + "     peptides"
//                    + "     children"
//                    + "     parents"
//                    + "     accessions"
//                    + "     allAccessions"
                    + ");");

            stmt.execute("CREATE TABLE accessions ("
                    + "     id integer PRIMARY KEY,"
                    + "     accession text,"
                    + "     sequence text,"
                    + "     pGroup integer,"
                    + "     FOREIGN KEY(pGroup) references groups(id)"
                    + ");");
            stmt.execute("CREATE UNIQUE INDEX idx_accessions_accession "
                    + "ON accessions (accession);");
            stmt.execute("CREATE TABLE accessionsToFiles ("
                    + "     accession integer,"
                    + "     file integer"
                    + ");");
            stmt.execute("CREATE TABLE accessionsToSearchDBs ("
                    + "     accession integer,"
                    + "     searchDB integer"
                    + ");");

            connection.commit();

            result = true;
        } catch (SQLException e) {
            LOGGER.error(e);
            result = false;
        }

        return result;
    }


    /**
     * Returns the accession ID from the SQLite DB
     *
     * @param acc
     * @return
     */
    private Long getAccessionIDfromDB(String acc) {
        Long accId = null;

        try (PreparedStatement pstmt = connection.prepareStatement(sqlGetAccessionID)) {
            pstmt.setString(1, acc);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    accId = rs.getLong(1);
                }
            }
        } catch (SQLException e) {
            LOGGER.error(e);
            accId = null;
        }

        return accId;
    }


    @Override
    public Accession getAccession(String acc) {
        Long accId = getAccessionIDfromDB(acc);

        if (accId != null) {
            return getAccession(accId);
        } else {
            return null;
        }
    }


    @Override
    public Accession getAccession(Long accId) {
        return accessions.get(accId);
    }


    @Override
    public Accession insertNewAccession(String accession, String dbSequence) {
        Accession acc = null;
        ResultSet rs = null;

        try (PreparedStatement pstmt = connection.prepareStatement(sqlNewAccession,
                Statement.RETURN_GENERATED_KEYS)) {
            long accId = 0;

            pstmt.setString(1, accession);
            pstmt.setString(2, dbSequence);
            pstmt.executeUpdate();

            // get the accession id
            rs = pstmt.getGeneratedKeys();
            if (rs.next()) {
                accId = rs.getLong(1);
            }

            connection.commit();

            LOGGER.debug(accId + " accession " + accession);

            acc = new Accession(accId, accession, dbSequence);
            accessions.put(accId, acc);
        } catch (SQLException e) {
            LOGGER.error(e);
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
            } catch (SQLException e) {
                LOGGER.error(e);
            }
        }

        return acc;
    }


    @Override
    public int getNrAccessions() {
        return accessions.size();
    }


    @Override
    public Set<Long> getAllAccessionIDs() {
        return accessions.keySet();
    }


    @Override
    public Peptide getPeptide(String sequence) {
        Long pepId = peptideSequencesToIDs.get(sequence);
        if (pepId != null) {
            return getPeptide(pepId);
        } else {
            return null;
        }
    }


    @Override
    public Peptide getPeptide(Long peptideID) {
        return peptides.get(peptideID);
    }


    @Override
    public Peptide insertNewPeptide(String sequence) {
        Peptide peptide;
        Long id = (long)peptides.size()+1;

        peptide = new Peptide(id, sequence);
        peptides.put(id, peptide);
        peptideSequencesToIDs.put(sequence, id);

        return peptide;
    }


    @Override
    public int getNrPeptides() {
        return peptides.size();
    }


    @Override
    public Set<Long> getAllPeptideIDs() {
        return peptides.keySet();
    }


    @Override
    public PeptideSpectrumMatch getPeptideSpectrumMatch(Long psmId) {
        return spectra.get(psmId);
    }


    @Override
    public PeptideSpectrumMatch createNewPeptideSpectrumMatch(Integer charge,
            double massToCharge, double deltaMass, Double rt, String sequence,
            int missed, String sourceID, String spectrumTitle,
            PIAInputFile file, SpectrumIdentification spectrumID) {
        PeptideSpectrumMatch psm;
        Long id = spectra.size() + 1L;

        psm = new PeptideSpectrumMatch(id, charge, massToCharge, deltaMass, rt,
                sequence, missed, sourceID, spectrumTitle,
                file, spectrumID);

        return psm;
    }


    @Override
    public void insertCompletePeptideSpectrumMatch(PeptideSpectrumMatch psm) {
        if (spectra.put(psm.getID(), psm) != null) {
            LOGGER.warn("spectrum was already in list, this should not have happened! "
                    + psm.getSequence());
        }
    }


    @Override
    public int getNrPeptideSpectrumMatches() {
        return spectra.size();
    }


    @Override
    public Set<Long> getAllPeptideSpectrumMatcheIDs() {
        return spectra.keySet();
    }


    @Override
    public Set<Peptide> getPeptidesFromConnectionMap(String acc) {
        Long accId = getAccessionIDfromDB(acc);

        if ((accId != null) && accPepMapIDs.containsKey(accId)) {
            return accPepMapIDs.get(accId).stream().map(pepId -> peptides.get(pepId)).collect(Collectors.toSet());
        } else {
            return null;
        }
    }


    @Override
    public Set<Accession> getAccessionsFromConnectionMap(String pep) {
        Long pepId = peptideSequencesToIDs.get(pep);
        if ((pepId != null) && pepAccMapIDs.containsKey(pepId)) {
            return pepAccMapIDs.get(pepId).stream().map(accId -> accessions.get(accId)).collect(Collectors.toSet());
        } else {
            return null;
        }
    }


    @Override
    public Set<Long> getPepIDsFromConnectionMap(Long accId) {
        return accPepMapIDs.get(accId);
    }


    @Override
    public Set<Long> getAccIDsFromConnectionMap(Long pepId) {
        return pepAccMapIDs.get(pepId);
    }


    @Override
    public void addAccessionPeptideConnection(Accession accession, Peptide peptide) {
        Long pepId = peptide.getID();
        Long accId = accession.getID();

        if (!accPepMapIDs.containsKey(accId)) {
            if (accessions.containsKey(accId) && peptides.containsKey(pepId)) {
                accPepMapIDs.put(accId, new HashSet<>());
            } else {
                // this was called erroneous, insert a null (which will provoke a NullPointerException)
                LOGGER.error("accession or peptide was not inserted into the compiler");
                accPepMapIDs.put(accId, null);
            }
        }

        if (!pepAccMapIDs.containsKey(pepId)) {
            if (accessions.containsKey(accId) && peptides.containsKey(pepId)) {
                pepAccMapIDs.put(pepId, new HashSet<>());
            } else {
                // this was called erroneous, insert a null (which will provoke a NullPointerException)
                LOGGER.error("accession or peptide was not inserted into the compiler");
                pepAccMapIDs.put(pepId, null);
            }
        }

        accPepMapIDs.get(accId).add(pepId);
        pepAccMapIDs.get(pepId).add(accId);
    }


    @Override
    public void clearConnectionMap() {
        accPepMapIDs.clear();
        pepAccMapIDs.clear();
    }


    @Override
    public void finish() {
        // nothing to do here
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                LOGGER.error(e);
            }
        }
    }


    /**
     * Write out the intermediate structure into the SQLite database.
     *
     * @param fileName
     * @throws IOException
     */
    public final void writeSQLite() throws IOException {



    }



}
