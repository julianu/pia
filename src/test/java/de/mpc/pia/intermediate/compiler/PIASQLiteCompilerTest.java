package de.mpc.pia.intermediate.compiler;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.SQLException;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;


public class PIASQLiteCompilerTest {

    private File mzid55mergeTandem;
    private File mzid55mergeMascot;

    private String piaIntermediateFileName = "/tmp/PIASQLiteCompilerTest.pia.sqlite";


    @Before
    public void setUp() {
        mzid55mergeTandem = new File(PIASQLiteCompilerTest.class.getResource("/55merge_tandem.mzid").getPath());
        mzid55mergeMascot = new File(PIASQLiteCompilerTest.class.getResource("/55merge_mascot_full.mzid").getPath());
    }


    @Test
    public void testPIASQLiteCompiler() throws IOException, SQLException {
        File piaIntermediateFile = File.createTempFile(piaIntermediateFileName, null);
        PIACompiler piaCompiler = new PIASQLiteCompiler(piaIntermediateFile.getAbsolutePath());

        assertTrue("X!TAndem file could not be parsed",
                piaCompiler.getDataFromFile("tandem", mzid55mergeTandem.getAbsolutePath(), null, null));
/*
        assertTrue("Mascot file could not be parsed",
                piaCompiler.getDataFromFile("mascot", mzid55mergeMascot.getAbsolutePath(), null, null));

        piaCompiler.buildClusterList();
        piaCompiler.buildIntermediateStructure();

        piaCompiler.setName("testFile");

        File piaIntermediateFile = File.createTempFile(piaIntermediateFileName, null);

        // test writing using the file object
        piaCompiler.writeOutXML(piaIntermediateFile);
        piaIntermediateFile.delete();
*/
        piaCompiler.finish();
    }
}
