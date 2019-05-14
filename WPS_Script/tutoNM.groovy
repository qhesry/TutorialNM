package org.orbisgis.orbiswps.scripts.scripts.NoiseModelling

import org.orbisgis.orbiswps.groovyapi.input.*
import org.orbisgis.orbiswps.groovyapi.output.*
import org.orbisgis.orbiswps.groovyapi.process.*

import org.noise_planet.noisemodelling.propagation.ComputeRaysOut
import org.noise_planet.noisemodelling.propagation.IComputeRaysOut
import org.noise_planet.noisemodelling.propagation.PropagationPath
import org.noise_planet.noisemodelling.propagation.PropagationProcessData
import org.noise_planet.noisemodelling.propagation.PropagationProcessPathData
import org.noise_planet.noisemodelling.propagation.jdbc.PointNoiseMap

import groovy.sql.Sql
import java.sql.Connection
import java.sql.DriverManager
import org.h2gis.utilities.wrapper.ConnectionWrapper

import org.locationtech.jts.geom.Coordinate
import org.noise_planet.noisemodelling.propagation.KMLDocument
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream
import java.text.DecimalFormat
import org.h2gis.api.EmptyProgressVisitor

/********************/
/** Process method **/
/********************/

/**
 * @author Quentin HESRY
 */
@Process(
        title = "Tutorial_NM",
        description = "Tutorial NM",
        keywords = ["NM","Tutorial","NM"],
        properties = ["DBMS_TYPE", "H2GIS", "DBMS_TYPE", "POSTGIS"],
        version = "1.0",
        identifier = "orbisgis:wps:NM:delaunayGrid"
)
def processing() {

    String h2_url = "jdbc:h2://home/quentin/Bureau/OrbisGIS/database;DB_CLOSE_DELAY=30;DEFRAG_ALWAYS=TRUE"
    String user_name = "sa"
    String user_password = ""

    String sources_table_name = sourcesTableName
    String receivers_table_name = receiversTableName
    String building_table_name = buildingTableName

    int reflexion_order = Integer.valueOf(reflexionOrder)
    double max_src_dist = Double.valueOf(maxSrcDistance)
    double max_ref_dist = Double.valueOf(maxRefDistance)
    double wall_alpha = Double.valueOf(wallAlpha)
    int n_thread = Integer.valueOf(threadNumber)

    boolean compute_vertical_diffraction = computeVertical
    boolean compute_horizontal_diffraction = computeHorizontal

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


    //SQL pour l'importation des tables depuis le tuto
    /*CALL SHPREAD('/home/quentin/Bureau/TutorialNM-master/InputFiles/Buildings.shp','buildings_zone');
    CALL SHPREAD('/home/quentin/Bureau/TutorialNM-master/InputFiles/Receivers.shp','Receivers');
    CALL SHPREAD('/home/quentin/Bureau/TutorialNM-master/InputFiles/Sound_source.shp','Sound_source');

    create spatial index on buildings_zone(the_geom);
    create spatial index on Receivers(the_geom);
    create spatial index on Sound_source(the_geom);*/

    List<ComputeRaysOut.verticeSL> allLevels = new ArrayList<>() // ca c'est la table avec les atténuations
    ArrayList<PropagationPath> propaMap2 = new ArrayList<>() // ca c'est la table avec tous les rayons, attention gros espace memoire !

    // ----------------------------------
    // Et la on commence la boucle sur les simus
    // ----------------------------------

    System.out.println("Run ...")
    // Configure noisemap with specified receivers
    //-----------------------------------------------------------------
    // ----------- ICI On calcul les rayons entre sources et recepteurs (c est pour ça r=0, on le fait qu'une fois)
    //-----------------------------------------------------------------

    PointNoiseMap pointNoiseMap = new PointNoiseMap(building_table_name, sources_table_name, receivers_table_name)
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
    //jdbcComputeRaysOut.workspace_output = workspace_output

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

    literalOutput = i18n.tr("Process done !")
}

/**********************/
/** INPUT Parameters **/
/**********************/

@JDBCTableInput(
	title = "Building table",
	description = "The table with the buildings",
	identifier = "BuildingTableName")
String buildingTableName

@JDBCTableInput(
	title = "Source Table",
	description = "The table with the sources",
	identifier = "SourcesTableName")
String sourcesTableName

@JDBCTableInput(
	title = "Receivers Table",
	description = "The table with the receivers",
	identifier = "ReceiversTableName")
String receiversTableName

@LiteralDataInput(
    title = "Reflexion order",
    description = "The reflexion order",
    minOccurs = 1)
String reflexionOrder = "1"

@LiteralDataInput(
    title = "Max source distance",
    description = "The max source distance",
    minOccurs = 1)
String maxSrcDistance = "1000"

@LiteralDataInput(
    title = "Max ref distance",
    description = "The Max ref distance",
    minOccurs = 1)
String maxRefDistance = "1000"

@LiteralDataInput(
    title = "Wall alpha",
    description = "The Wall alpha",
    minOccurs = 1)
String wallAlpha = "0.1"

@LiteralDataInput(
    title = "Thread number",
    description = "The number of thread",
    minOccurs = 1)
String threadNumber = "10"

/*@LiteralDataInput(
    title = "Database Path",
    description = "The path to the database",
    minOccurs = 1)
String database_path = "/home/quentin/Bureau/OrbisGIS/database"*/

@LiteralDataInput(
    title = "Compute vertical diffraction",
    description = "Compute vertical diffraction",
    minOccurs = 1)
Boolean computeVertical

@LiteralDataInput(
    title = "Compute horizontal diffraction",
    description = "Compute horizontal diffraction",
    minOccurs = 1)
Boolean computeHorizontal

/** Output message. */
@LiteralDataOutput(
        title = "Output message",
        description = "The output message.",
        identifier = "literalOutput")
String literalOutput

class JDBCComputeRaysOut implements PointNoiseMap.IComputeRaysOutFactory {
    def exportReceiverRay = [1, 2].toArray() // primary key of receiver to export
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

class RayOut extends ComputeRaysOut {
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
            if(jdbccomputeraysout.exportReceiverRay.contains(receiverId)) {
                // Export rays
                jdbccomputeraysout.kmlDocument.writeRays(propagationPaths)
            }
        }
        //propagationPaths.clear()
    }
}
