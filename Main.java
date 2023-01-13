import com.mongodb.BasicDBObject;
import com.mongodb.client.*;
import org.bson.Document;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

public class Main {
    static JSONObject parseGeo(Document geo) { // разбор геометрии в json
        JSONObject geoObj = new JSONObject();
        if (geo != null) {
            String type = (String) geo.get("type");
            if (!type.equals("WTF")) {
                ArrayList coors = (ArrayList) geo.get("coordinates");
                if (coors != null) {
                    geoObj.put("type", type);
                    geoObj.put("coordinates", coors);
                } else {
                    geoObj = null;
                }
            } else {
                geoObj = null;
            }
        }
        return geoObj;
    }

    public static void main(String[] args) throws SQLException, ParseException, IOException {
        if (args[0] != null) {
            long startT = System.currentTimeMillis();
            try {
                LogManager.getLogManager().readConfiguration(Main.class.getResourceAsStream("/logProperties"));
            } catch (IOException e) {
                System.err.println("Could not setup logger configuration: " + e.toString());
            }
            Logger log = Logger.getLogger(Main.class.getName());

            JSONParser parser = new JSONParser();
            Reader reader = new InputStreamReader(new FileInputStream("settings.json"), StandardCharsets.UTF_8);

            JSONObject obj = (JSONObject) parser.parse(reader);
            String ConnectionStringMongo = (String) obj.get("ConnectionStringMongo");
            JSONObject ConnectionStringPostgre = (JSONObject) obj.get("ConnectionStringPostgre");
            String URL = (String) ConnectionStringPostgre.get("URL");
            String User = (String) ConnectionStringPostgre.get("user");
            String Password = (String) ConnectionStringPostgre.get("password");
            ArrayList<String> priorityOrder = (ArrayList<String>) obj.get("priorityOrder");
            String Collection = (String) obj.get("collection");
            String outputFileName = (String) obj.get("outputFileName");

            JSONObject filtersJSON = (JSONObject) obj.get("filter");
            BasicDBObject filter = BasicDBObject.parse(filtersJSON.toString());

//
            File file = new File(outputFileName + ".geojson");
            file.createNewFile();
            Writer wr = new OutputStreamWriter(new FileOutputStream(file), StandardCharsets.UTF_8);

            JSONArray features = new JSONArray();
            JSONObject featureCollection = new JSONObject();
            featureCollection.put("type", "FeatureCollection");
            JSONObject feature = new JSONObject();

            File fileW = new File(outputFileName + "Weighted.geojson");
            fileW.createNewFile();
            Writer wrW = new OutputStreamWriter(new FileOutputStream(fileW), StandardCharsets.UTF_8);

            JSONArray featuresW = new JSONArray();
            JSONObject featureCollectionW = new JSONObject();
            featureCollectionW.put("type", "FeatureCollection");
            JSONObject featureW = new JSONObject();

            File file2 = new File("clustIN4.geojson");
            file2.createNewFile();
            Writer wr2 = new OutputStreamWriter(new FileOutputStream(file2), StandardCharsets.UTF_8);

            JSONArray features2 = new JSONArray();
            JSONObject featureCollection2 = new JSONObject();
            featureCollection2.put("type", "FeatureCollection");
            JSONObject feature2 = new JSONObject();
            JSONObject properties2 = new JSONObject();
//

            Connection conn = DriverManager.getConnection(URL, User, Password);
            MongoClient mongo = MongoClients.create(ConnectionStringMongo);

            MongoDatabase db = mongo.getDatabase(args[0]);
            MongoCollection<Document> hClusters = db.getCollection(Collection);

            MongoCursor<Document> cursor = hClusters.find(filter).iterator();


            // изначально был тип String, поменяла для сборки в мультиполигон без хранимки
            ArrayList<JSONObject> geometries = new ArrayList<>(); // сюда кладется вся геометрия из AllVertexes

            ArrayList<String> finalGeometries = new ArrayList<>(); // тут геометрия скомпонованная по источникам в мультиполигоны
            Set<String> sourcesForCentr = new LinkedHashSet<>(); // источники для центрирования упорядоченные // LinkedHashSet - неповторяющиеся объекты в порядке добавления

            String queryMain = "{call rk_hcluster(?, ?, ?)}";
            CallableStatement ps = conn.prepareCall(queryMain);

            int countProcessed = 0; // счетчик обработанных объектов хранимкой
            int countWeighted = 0; // счетчик объектов выбранных по весу

            // хранимка для создания мультиполигона // больше не нужна
          /* String query = "{call rk_alignn(?)}";
            CallableStatement ps2 = conn.prepareCall(query);*/

            while (cursor.hasNext()) { // пробег по базе h_clusters
                Document d = cursor.next();
                ArrayList ids = (ArrayList) d.get("allVertexes"); // поле в котором хранятся ссылки на объекты кластера
                // allVertexes - массив с записями в формате источник_id

                ArrayList<String> sources = new ArrayList<>(); // все источники по порядку с повторами

                for (int i = 0; i < ids.size(); i++) { // проход по массиву с ссылками на объекты
                    // в конце этого цикла передать массив геометрий в пг и обновить его
                    String str = (String) ids.get(i);
                    String[] s = str.split("_"); // название базы + id
                    String collection = s[0] + "_houses";

                    if (!collection.equals("lands_houses")) { // исключаем ЗУ
                        BasicDBObject IDfilter = new BasicDBObject();
                        IDfilter.put("ID", s[1]);
                        MongoCollection<Document> houses = db.getCollection(collection); // поиск документа в коллекции источника по id
                        MongoCursor<Document> c = houses.find(IDfilter).iterator();
                        while (c.hasNext()) { // получение геометрии
                            Document doc = c.next();
                            Document geoDoc = (Document) doc.get("Geometry");
                            JSONObject geoObj = parseGeo(geoDoc);
                            if (geoObj != null && !geoObj.isEmpty()) {
                                if (!geoObj.get("type").equals("Point")) {

                                    sources.add(s[0]); // нужен для разделения геометрии на массивы по источникам
                                    sourcesForCentr.add(s[0]); // нужен для определения номера приоритетного истоника для центрирования
                                    geometries.add(geoObj);

                                    //
                                    feature2.put("type", "Feature");
                                    feature2.put("geometry", geoObj);
                                    properties2.put("ID", d.getObjectId("_id").toString());
                                    feature2.put("properties", properties2);
                                    features2.add(feature2);
                                    feature2 = new JSONObject();
                                    properties2 = new JSONObject();
                                    //
                                }
                            }
                        }
                    }
                } // конец цикла для пробега по массиву с ссылками на источники

                // разделение геометрии на массивы по источникам
                ArrayList oneSourceGeom = new ArrayList<>();
                ArrayList parts = new ArrayList<>();
                int countParts = 1;
                for (int i = 0; i < sources.size() - 1; i++) {
                    if (sources.get(i).equals(sources.get(i + 1))) {
                        countParts++;
                    } else {
                        parts.add(countParts);
                        countParts = 1;
                    }
                }
                countParts = 0;
                for (int j = 0; j < parts.size(); j++) {
                    countParts += (int) parts.get(j);
                }
                int remaining = sources.size() - countParts;
                if (remaining > 0) {
                    parts.add(remaining);
                }

                int start = 0;
                int end = 0;
                int k = 0;
                for (int j = 0; j < parts.size(); j++) {
                    end = (int) parts.get(j) + start;
                    for (k = start; k < end; k++) {
                        oneSourceGeom.add(geometries.get(k));
                    }
                    // массив oneSourceGeom содержит геометрию из одного источника
                    // каждый источник собирается в мультиполигон в пг хранимке
                    /*final Array stringsArray = conn.createArrayOf(
                            "varchar",
                            oneSourceGeom.toArray(new String[oneSourceGeom.size()]));
                    ps2.setArray(1, stringsArray);
                    ResultSet rs = ps2.executeQuery();
                    while (rs.next()) { // добавить проверку на null
                        String ans = rs.getString(1);
                        finalGeometries.add(ans); // объединенная геометрия
                        System.out.println("ANS: " + ans);
                    }*/

                    // сборка объектов из одного источника в мультиполигон
                    String coor = "{\"type\": \"MultiPolygon\", \"coordinates\": [";
                    for (int i = 0; i < oneSourceGeom.size(); i++) {
                        JSONObject temp = (JSONObject) oneSourceGeom.get(i);
                        coor = coor + temp.get("coordinates").toString() + ',';
                    }
                    String t = coor.substring(0, coor.length() - 1);
                    t = t + ']' + '}';
                    JSONObject Multi = (JSONObject) parser.parse(t);
                    finalGeometries.add(Multi.toString());
                    oneSourceGeom = new ArrayList<>();
                    start = k;
                }

                // передать массив в пг получить ответ обновить массив

                // определение номер приоритетного источника в массиве
                Object[] sourcesForCentrArr = sourcesForCentr.toArray();
                int sourceNum = -1;
                for (int i = 0; i < priorityOrder.size(); i++) {
                    if (sourcesForCentr.contains(priorityOrder.get(i))) {
                        for (int j = 0; j < sourcesForCentrArr.length; j++) {
                            if (priorityOrder.get(i).equals(sourcesForCentrArr[j]) && sourceNum == -1) {
                                sourceNum = j;
                                break;
                            }
                        }
                    }
                }

                if (finalGeometries.size() > 1) {
                    //
                    final Array stringsArray = conn.createArrayOf(
                            "varchar",
                            finalGeometries.toArray(new String[finalGeometries.size()]));
                    ps.setArray(1, stringsArray);
                    ps.setInt(2, finalGeometries.size());
                    ps.setInt(3, sourceNum);

                    ResultSet rs = ps.executeQuery();
                    while (rs.next()) {
                        if ((rs.getString(1) != null)) {
                            JSONObject g2 = (JSONObject) parser.parse(rs.getString(1));
// запись построенных объектов
                            countProcessed++;
                            feature.put("type", "Feature");
                            feature.put("geometry", g2);
                            features.add(feature);
                            feature = new JSONObject();

                        } else {
// запись взвешенных объектов
                            ArrayList<String> sourcesWeightList = new ArrayList<>();
                            // cписок строк название источника + Weight
                            for (int i = 0; i < sourcesForCentrArr.length; i++) {
                                sourcesWeightList.add(sourcesForCentrArr[i] + "Weight");
                            }
                            TreeMap<Double, String> weightsSortedMap = new TreeMap<>();

                            for (int i = 0; i < sourcesWeightList.size(); i++) {
                                var temp = d.get(sourcesWeightList.get(i)); // получение веса
                                if (temp.getClass().toString().equals("class java.lang.Double")) {
                                    weightsSortedMap.put(d.getDouble(sourcesWeightList.get(i)), sourcesWeightList.get(i));
                                } else {
                                    weightsSortedMap.put(Double.valueOf(d.getInteger(sourcesWeightList.get(i))), sourcesWeightList.get(i));
                                }
                            }
                            ArrayList<String> sourcesNames = new ArrayList<String>(weightsSortedMap.values());
                            //ArrayList<Double> sourcesWeights = new ArrayList<Double>(weightsSortedMap.keySet());

                            boolean isMax = false;
                            for (int i = sourcesNames.size() - 1; i > -1 && !isMax; i--) {
                                BasicDBObject weightFilter = new BasicDBObject();
                                weightFilter.put("ID", d.get(sourcesNames.get(i).replace("Weight", "ID")));
                                MongoCollection<Document> houses = db.getCollection(sourcesNames.get(i).replace("Weight", "_houses")); // поиск документа в коллекции источника по id
                                MongoCursor<Document> c = houses.find(weightFilter).iterator();
                                while (c.hasNext()) { // получение геометрии
                                    Document doc = c.next();
                                    Document geoDoc = (Document) doc.get("Geometry");
                                    JSONObject geoObj = parseGeo(geoDoc);
                                    if (geoObj != null && !geoObj.isEmpty()) {
                                        if (!geoObj.get("type").equals("Point")) {
                                            countWeighted++;
                                            isMax = true;
                                            featureW.put("type", "Feature");
                                            featureW.put("geometry", geoObj);
                                            featuresW.add(featureW);
                                            featureW = new JSONObject();
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                finalGeometries = new ArrayList<>();
                geometries = new ArrayList<>();
                sourcesForCentr = new LinkedHashSet<>();
            }

//
            featureCollection.put("features", features);
            wr.write(featureCollection.toString());
            wr.flush();
            wr.close();

            featureCollectionW.put("features", featuresW);
            wrW.write(featureCollectionW.toString());
            wrW.flush();
            wrW.close();

            featureCollection2.put("features", features2);
            wr2.write(featureCollection2.toString());
            wr2.flush();
            wr2.close();
//

            mongo.close();
            conn.close();
            long finish = System.currentTimeMillis();
            long elapsed = finish - startT;

            log.log(Level.SEVERE, "The count of processed by function objects: " + countProcessed);
            log.log(Level.SEVERE, "The count of objects chosen by weight: " + countWeighted);
            log.log(Level.SEVERE, "Duration, seconds: " + elapsed / 1000);
            System.out.println("Прошло времени, с: " + elapsed / 1000);
        }
    }
}
