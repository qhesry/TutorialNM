package org.noise_planet.nm_run_multiples_instances

/** author : Aumond Pierre ; Nicolas Fortin

 Ce code permet de faire une simulation

 Il s'agit d'un code pour un futur Tutorial

 Les fichiers Input se trouvent dans : .\InputFiles
 Une partie des résultats se trouve dans : .\Results
 L'autre partie est 'print' en fin de run

 **/

// Importation des librairies
import groovy.sql.Sql

import org.h2gis.utilities.wrapper.ConnectionWrapper
import org.locationtech.jts.geom.Coordinate
import org.noise_planet.noisemodelling.propagation.KMLDocument
import java.sql.Connection
import java.sql.DriverManager

import org.noise_planet.noisemodelling.propagation.ComputeRaysOut
import org.noise_planet.noisemodelling.propagation.IComputeRaysOut
import org.noise_planet.noisemodelling.propagation.PropagationPath
import org.noise_planet.noisemodelling.propagation.PropagationProcessData
import org.noise_planet.noisemodelling.propagation.PropagationProcessPathData
import org.noise_planet.noisemodelling.propagation.jdbc.PointNoiseMap

import java.text.DecimalFormat
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream
import org.h2gis.api.EmptyProgressVisitor

import groovy.transform.SourceURI
import java.nio.file.Path
import java.nio.file.Paths


class OneRun {
    static void main(String[] args) {
        OneRun oneRun = new OneRun()
        oneRun.run()
    }

    void run() {
/**
 ///////////////////////////////////////////
 // Paramètres d'entrée et initialisations //
 ///////////////////////////////////////////

 */
        @SourceURI
        URI sourceUri
        Path scriptLocation = Paths.get(sourceUri)
        String rootPath = scriptLocation.getParent().getParent().getParent().getParent().toString()
        String rootPath2 = scriptLocation.getParent().getParent().getParent().getParent().getParent().getParent().toString()

        String workspace_output = rootPath2+"/Results/"

        System.out.println("Total memory (bytes): " +
                Runtime.getRuntime().totalMemory())

        boolean clean = true // reset the database

        boolean loadRays = false
        boolean saveRays = false

        String h2_url
        String user_name
        String user_password

        // Ouverture de la base de données H2 qui recueillera les infos
        def database_path = rootPath2+"/DB/database"
        def database_file = new File(database_path+".mv.db")
        if (database_file.exists()) {
            database_file.delete()
        }
        h2_url = "jdbc:h2:/"+database_path+";DB_CLOSE_DELAY=30;DEFRAG_ALWAYS=TRUE"
        user_name = "sa"
        user_password = ""


        // Noms des tables en entrée et des attributs (.shp)
        String sources_table_name2 = "Sound_source"
        String receivers_table_name2 = "Receivers"
        String building_table_name2 = "Buildings"


        String shpPath = rootPath2 + "/InputFiles/"
        // Paramètres de propagation
        int reflexion_order = 1
        double max_src_dist = 1000
        double max_ref_dist = 1000
        double wall_alpha = 0.1
        int n_thread = 10

        boolean compute_vertical_diffraction = true
        boolean compute_horizontal_diffraction = true

        // rose of favourable conditions
        //double[] favrose = [0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00]


/**
 ///////////////////////////////////////////
 // FIN Paramètres d'entrée et initialisations //
 ///////////////////////////////////////////

 */
        boolean H2 =true
        // Ouverture de la base de donnees PostGreSQL qui recueillera les infos
        ArrayList<Connection> connections = new ArrayList<>()

        // Connexion à la base
        def driver
        if (!H2) {
            driver = 'org.orbisgis.postgis_jts.Driver'
        } else {
            driver = 'org.h2.Driver'
        }
        Class.forName(driver)
        Connection connection = new ConnectionWrapper(DriverManager.getConnection(h2_url, user_name, user_password))
        connections.add(connection)
        def sql = Sql.newInstance(h2_url, user_name, user_password, driver)

        if(H2) {
            sql.execute("CREATE ALIAS IF NOT EXISTS H2GIS_SPATIAL FOR \"org.h2gis.functions.factory.H2GISFunctions.load\";")
            sql.execute("CALL H2GIS_SPATIAL();")
        }

        try {
            System.out.println("Init")


            //////////////////////
            // Ici on debute les calculs
            //////////////////////


            String sources_table_name = "ROADS_SRC_ZONE" // ne pas modifier
            String receivers_table_name = "RECEIVERS2" // ne pas modifier

            // If clean = true, recompute input tables

            if (clean) {
                System.out.println("Clean database...")
                // Nettoyage de la base de données
                sql.execute(new File(rootPath + "/sql/Cleaning_database.sql").text)
                // import des shapefiles ici
                sql.execute('drop table ZONE_CENSE_2KM if exists ')
                sql.execute('drop table buildings_zone if exists ')
                sql.execute('drop table receivers2 if exists ')
                sql.execute('drop table land_use_zone_capteur2 if exists ')
                sql.execute('drop table dem_lite2 if exists ')
                sql.execute('drop table ROADS_SRC_ZONE if exists ')
                sql.execute('drop table ROADS_TRAFFIC_ZONE_CAPTEUR_format2 if exists ')

                // ------------------------------------------------------------ //
                // ----------- Initialisation et import des données ----------- //
                // -----------        (sol et des bâtiments)        ----------- //
                // ------------------------------------------------------------ //

                //sql.execute([f:shpPath+"zone.shp"], "CALL SHPREAD(:f,'zone_cense_2km')")
                sql.execute([f:shpPath+building_table_name2+".shp"],"CALL SHPREAD(:f,'buildings_zone')")
                sql.execute([f:shpPath+receivers_table_name2+ ".shp"],"CALL SHPREAD(:f,'"+receivers_table_name+"')")
                //sql.execute([f:shpPath+"occsol.shp"],"CALL SHPREAD(:f,'land_use_zone_capteur2')")
                //sql.execute([f:shpPath+"mnt2.shp"],"CALL SHPREAD(:f,'DEM_LITE2')")
                sql.execute([f:shpPath+sources_table_name2+".shp"],"CALL SHPREAD(:f,'"+sources_table_name+"')")
                //sql.execute([f:shpPath+"route_adapt2.shp"],"CALL SHPREAD(:f,'ROADS_TRAFFIC_ZONE_CAPTEUR_format2')")

                //sql.execute("create spatial index on zone_cense_2km(the_geom)")
                sql.execute("create spatial index on buildings_zone(the_geom)")
                sql.execute("create spatial index on "+receivers_table_name+"(the_geom)")
                //sql.execute("create spatial index on land_use_zone_capteur2(the_geom)")
               // sql.execute("create spatial index on dem_lite2(the_geom)")
                sql.execute("create spatial index on roads_src_zone(the_geom)")
                sql.execute("create spatial index on "+sources_table_name+"(the_geom)")
            }

            sql.execute(new File(rootPath + "/sql/LoadSrcsFromPGis.sql").text)
            sql.execute("delete from receiver_lvl_day_zone;")


            List<ComputeRaysOut.verticeSL> allLevels = new ArrayList<>() // ca c'est la table avec les atténuations
            ArrayList<PropagationPath> propaMap2 = new ArrayList<>() // ca c'est la table avec tous les rayons, attention gros espace memoire !

            // ----------------------------------
            // Et la on commence la boucle sur les simus
            // ----------------------------------
            if (!loadRays) {
                System.out.println("Run ...")
                // Configure noisemap with specified receivers
                //-----------------------------------------------------------------
                // ----------- ICI On calcul les rayons entre sources et recepteurs (c est pour ça r=0, on le fait qu'une fois)
                //-----------------------------------------------------------------

                PointNoiseMap pointNoiseMap = new PointNoiseMap("BUILDINGS_ZONE", "ROADS_SRC_ZONE", "RECEIVERS2")
                pointNoiseMap.setComputeHorizontalDiffraction(compute_horizontal_diffraction)
                pointNoiseMap.setComputeVerticalDiffraction(compute_vertical_diffraction)
                pointNoiseMap.setSoundReflectionOrder(reflexion_order)
                pointNoiseMap.setHeightField("HEIGHT")
                //pointNoiseMap.setDemTable("DEM_LITE2")
                pointNoiseMap.setMaximumPropagationDistance(max_src_dist)
                pointNoiseMap.setMaximumReflectionDistance(max_ref_dist)
                pointNoiseMap.setWallAbsorption(wall_alpha)
                //pointNoiseMap.setSoilTableName("LAND_USE_ZONE_CAPTEUR2")
                pointNoiseMap.setThreadCount(n_thread)
                JDBCComputeRaysOut jdbcComputeRaysOut = new JDBCComputeRaysOut()
                pointNoiseMap.initialize(connection, new EmptyProgressVisitor())
                pointNoiseMap.setComputeRaysOutFactory(jdbcComputeRaysOut)
                jdbcComputeRaysOut.workspace_output = workspace_output

                Set<Long> receivers_ = new HashSet<>()
                for (int i = 0; i < pointNoiseMap.getGridDim(); i++) {
                    for (int j = 0; j < pointNoiseMap.getGridDim(); j++) {
                        IComputeRaysOut out = pointNoiseMap.evaluateCell(connection, i, j, new EmptyProgressVisitor(), receivers_)

                        if (out instanceof ComputeRaysOut) {
                            allLevels.addAll(((ComputeRaysOut) out).getVerticesSoundLevel())
                            propaMap2.addAll(((ComputeRaysOut) out).getPropagationPaths())
                        }
                    }
                }

                jdbcComputeRaysOut.closeKML()

            }

            println("--------------------------")
            println("- Chemins de propagation -")
            println("--------------------------")
            for (int i=0;i< propaMap2.size() ; i++) {
                println("ReceiverId: " + propaMap2.get(i).idReceiver + "; SourceId: " + propaMap2.get(i).idSource)
                println("Taille rayon principal S-R: "+ new DecimalFormat("##.##").format(propaMap2.get(i).getSRList().get(0).d) + " m")
            }


            println("------------------------------")
            println("- Attenuation par couple S-R -")
            println("------------------------------")
            for (int i=0;i< allLevels.size() ; i++) {
                println("ReceiverId: " + allLevels.get(i).receiverId + "; SourceId: " + allLevels.get(i).sourceId+
                        "; 63Hz: "+new DecimalFormat("##.##").format(allLevels.get(i).value[0])+"dB ; 125Hz :"
                        + new DecimalFormat("##.##").format(allLevels.get(i).value[1])+"dB ; 250Hz :"
                        + new DecimalFormat("##.##").format(allLevels.get(i).value[2])+"dB ; 500Hz :"
                       + new DecimalFormat("##.##").format(allLevels.get(i).value[3])+"dB ; 1kHz :"
                        + new DecimalFormat("##.##").format(allLevels.get(i).value[4])+"dB ; 2kHz :"
                        + new DecimalFormat("##.##").format(allLevels.get(i).value[5])+"dB ; 4kHz :"
                       + new DecimalFormat("##.##").format(allLevels.get(i).value[6])+"dB ; 8kHz :"
                        + new DecimalFormat("##.##").format(allLevels.get(i).value[7]))
            }


            /*def qry = 'INSERT INTO RECEIVER_LVL_DAY_ZONE (IDRECEPTEUR, IDSOURCE,' +
                    'ATT63, ATT125, ATT250, ATT500, ATT1000,ATT2000, ATT4000, ATT8000) ' +
                    'VALUES (?,?,?,?,?,?,?,?,?,?);'
            sql.withBatch(100, qry) { ps ->
                for (int i=0;i< allLevels.size() ; i++) {

                        ps.addBatch(allLevels.get(i).receiverId, allLevels.get(i).sourceId,
                                allLevels.get(i).value[0], allLevels.get(i).value[1], allLevels.get(i).value[2],
                                allLevels.get(i).value[3], allLevels.get(i).value[4], allLevels.get(i).value[5],
                                allLevels.get(i).value[6], allLevels.get(i).value[7])
                }
            }*/



        } finally {

            sql.close()
            connection.close()
        }
    }

    private static class JDBCComputeRaysOut implements PointNoiseMap.IComputeRaysOutFactory {
        long exportReceiverRay = 1 // primary key of receiver to export
        KMLDocument kmlDocument
        ZipOutputStream compressedDoc
        String workspace_output

        void closeKML(){
            if(kmlDocument != null) {
                kmlDocument.writeFooter()
                compressedDoc.closeEntry()
                compressedDoc.close()
            }
        }

        @Override
        IComputeRaysOut create(PropagationProcessData threadData, PropagationProcessPathData pathData) {
            closeKML()
            kmlDocument = null
            if(true || !threadData.receivers.isEmpty()) {
                compressedDoc = new ZipOutputStream(new FileOutputStream(
                        String.format(workspace_output+"domain_%d.kmz", threadData.cellId)))
                compressedDoc.putNextEntry(new ZipEntry("doc.kml"))
                kmlDocument = new KMLDocument(compressedDoc)
                kmlDocument.writeHeader()
                kmlDocument.setInputCRS("EPSG:2154")
                kmlDocument.setOffset(new Coordinate(0,0,0.1))
                kmlDocument.writeTopographic(threadData.freeFieldFinder.getTriangles(), threadData.freeFieldFinder.getVertices())
                kmlDocument.writeBuildings(threadData.freeFieldFinder)
            }

            return new RayOut(true, pathData, threadData, this)
        }
    }

    private static class RayOut extends ComputeRaysOut {
        JDBCComputeRaysOut jdbccomputeraysout

        RayOut(boolean keepRays, PropagationProcessPathData pathData, PropagationProcessData processData, JDBCComputeRaysOut jdbccomputeraysout) {
            super(keepRays, pathData, processData)
            this.jdbccomputeraysout = jdbccomputeraysout

        }

        @Override
        double[] computeAttenuation(PropagationProcessPathData pathData, long sourceId, double sourceLi, long receiverId, List<PropagationPath> propagationPath) {
            double[] attenuation = super.computeAttenuation(pathData, sourceId, sourceLi, receiverId, propagationPath)
            return attenuation
        }

        @Override
        void finalizeReceiver(long receiverId) {
            super.finalizeReceiver(receiverId)
            if(jdbccomputeraysout.kmlDocument != null && receiverId < inputData.receiversPk.size()) {
                receiverId = inputData.receiversPk.get((int)receiverId)
                if(receiverId == jdbccomputeraysout.exportReceiverRay) {
                    // Export rays
                    jdbccomputeraysout.kmlDocument.writeRays(propagationPaths)
                }
            }
            //propagationPaths.clear()
        }
    }

}


