/**
 * Created by xxy on 17/3/24.
 */

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;


public class DCube {
    private static final Map<String, String> ENV = System.getenv();
    private static int attributeNum;
    private static int k;
    private static String denMeature;
    private static String selectDimension;
    private static Connection c;
    private static double productOfCardinalitiesOfAll;
    private static Map<String, Integer> cardinalities = new HashMap<>();
    private static String colNames = "";
    private static String TMP_FILE_PATH;

    public static Connection openDatabase(String port, String db, String userName) {
        Connection c = null;
        try {
            Class.forName("org.postgresql.Driver");
            c = DriverManager
                    .getConnection("jdbc:postgresql://localhost:" + port + "/" + db,
                            userName, "");

        } catch (Exception e) {
            e.printStackTrace();
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        System.out.println("Opened database successfully");
        return c;
    }

    public static void importData(String fileInputPath) {
        System.out.println("import data " + fileInputPath + " to database");

        Statement stmt = null;
        String sql = null;

        try {
            stmt = c.createStatement();

            // DROP table if exists previously
            sql = "DROP TABLE IF EXISTS R";
            stmt.executeUpdate(sql);

            // CREATE table R
            sql = "CREATE TABLE R ( ";
            sql += "col1" + " CHAR(100)";
            colNames += "(col1";
            for (int i = 2; i <= attributeNum; i++) {
                sql += ", col" + String.valueOf(i) + " CHAR(100)";
                colNames += ", col" + String.valueOf(i);
            }
            sql += ", cnt INT, del INT )";
            colNames += ", cnt, del)";
            stmt.executeUpdate(sql);
            System.out.println("Create R table done");

            sql = "COPY R FROM '" + fileInputPath + "' WITH DELIMITER ',' CSV";
            stmt.executeUpdate(sql);
            System.out.println("Import data done");

            sql = "CREATE INDEX ON R " + colNames;
            stmt.executeUpdate(sql);

            stmt.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static int getTableSize(String table) {
        Statement stmt;
        String sql;
        ResultSet rs;

        Integer size;

        // Find cardinality for Bi from cached memory
        if (table.charAt(0) == 'B' && !table.equals("Bprime")) {
            size = cardinalities.get(table);

            if (size != null) {
                return size;
            }
        }

        try {
            stmt = c.createStatement();
            sql = "SELECT COUNT(*) AS tuplenum FROM " + table;
            rs = stmt.executeQuery(sql);
            rs.next();

            // Cache cardinality in memory
            size = rs.getInt("tuplenum");
            // TODO
            cardinalities.put(table, size);
            return size;

        } catch (Exception e) {
            e.printStackTrace();
        }
        return -1;
    }

    public static int getTableSize(int n, String table) {
        return getTableSize(table + String.valueOf(n));
    }

    public static int selectDimensionByCardinality() {
        int n = 1;
        int tupleNum = -1;

        for (int i = 1; i <= attributeNum; i++) {

            int tmp = getTableSize(i, "B");
            if (tmp > tupleNum) {
                tupleNum = tmp;
                n = i;
            }
        }

        return n;
    }

    public static int selectDimensionByDensity(double massB, double massR) throws SQLException {
        Statement stmt = c.createStatement();
        String sql;
        int i_tilde = 1;
        double massBPrime;
        double rho_tilde = Double.NEGATIVE_INFINITY, rho_prime, threshold;
        int sizeB;
        for (int i = 1; i <= attributeNum; i++) {
            sizeB = getTableSize(i, "B");
            if (sizeB > 0) {

                // Compute the low-mass tuples in ascending order
                sql = "DROP TABLE IF EXISTS Dt";
                stmt.executeUpdate(sql);
                sql = "CREATE TABLE Dt (a CHAR(100), mb DOUBLE PRECISION)";
                stmt.executeUpdate(sql);
                sql = "CREATE INDEX ON Dt (a, mb)";
                stmt.executeUpdate(sql);

                threshold = massB / sizeB;
                sql = "INSERT INTO Dt (" +
                        " SELECT a, mass FROM MB" +
                        " WHERE mass<=" + threshold +
                        " AND i=" + i +
                        " ORDER BY mass" +
                        " )";
                stmt.executeUpdate(sql);

                sql = "DROP TABLE IF EXISTS Bprime";
                stmt.executeUpdate(sql);
                sql = "CREATE TABLE Bprime AS TABLE B" + i;
                stmt.executeUpdate(sql);

                // Tuples are removed from low to high mass
                sql = "SELECT SUM(mb) AS mbsum FROM Dt";
                ResultSet rs = stmt.executeQuery(sql);
                rs.next();
                massBPrime = massB - rs.getDouble("mbsum");;
                rs.close();

                sql = "DELETE FROM Bprime WHERE col IN (SELECT a FROM Dt)";
                stmt.executeUpdate(sql);

                // Update density after all in Di are removed
                rho_prime = densityMeasure(massBPrime, massR, i);

                System.out.println("dim" + i + ", density: " + rho_prime);

                // Update max density and its dimension so far
                if (rho_prime > rho_tilde) {
                    rho_tilde = rho_prime;
                    i_tilde = i;
                }
            }
        }

        // Return the dimension with max density
        return i_tilde;
    }

    public static double densityMeasure(double MB, double MR) {

        if (denMeature.equals("ari")) {
            double tmp = 0.0;
            for (int i = 1; i <= attributeNum; i++) {
                tmp += (double) getTableSize(i, "B");
            }
            if (tmp == 0) {
                return -1;
            }
            tmp /= attributeNum;
            return MB / tmp;

        } else if (denMeature.equals("geo")) {
            double tmp = 1.0;
            for (int i = 1; i <= attributeNum; i++) {
                tmp *= (double) getTableSize(i, "B");
                if (tmp == 0) {
                    return -1;
                }
            }
            tmp = Math.pow(tmp, 1.0 / attributeNum);
            return MB / tmp;
        } else if (denMeature.equals("susp")) {
            if (MB == 0) {
                return -1;
            }
            double productOfCardinalitiesOfBlock = 1.0;
            for (int i = 1; i <= attributeNum; i++) {
                productOfCardinalitiesOfBlock *= (double) getTableSize(i, "B");
                if (productOfCardinalitiesOfBlock == 0) {
                    return -1;
                }
            }
//            System.out.println("MB="+MB +",MR="+MR+",prodB="+productOfCardinalitiesOfBlock+",prodR="+productOfCardinalitiesOfAll);
            double tmp = productOfCardinalitiesOfBlock / productOfCardinalitiesOfAll;
            return MB * (Math.log(MB / MR) - 1) + MR * tmp - MB * Math.log(tmp);
        } else {
            System.err.println("Incorrect density measure!");
        }
        return 0;
    }

    @SuppressWarnings("Duplicates")
    public static double densityMeasure(double MB, double MR, int dim) {

        if (denMeature.equals("ari")) {
            double tmp = 0.0;
            for (int i = 1; i <= attributeNum; i++) {
                if (i == dim) continue;
                tmp += (double) getTableSize(i, "B");
            }
            tmp += (double) getTableSize("Bprime");
            if (tmp == 0) return -1;
            tmp /= attributeNum;
            return MB / tmp;

        } else if (denMeature.equals("geo")) {
            double tmp = 1.0;
            for (int i = 1; i <= attributeNum; i++) {
                if (i == dim) continue;
                tmp *= (double) getTableSize(i, "B");
                if (tmp == 0) {
                    return -1;
                }
            }
            tmp *= (double) getTableSize("Bprime");
            if (tmp == 0) {
                return -1;
            }
            tmp = Math.pow(tmp, 1.0 / attributeNum);
            return MB / tmp;
        } else if (denMeature.equals("susp")) {
            if (MB == 0) {
                return -1;
            }
            double productOfCardinalitiesOfBlock = 1.0;
            for (int i = 1; i <= attributeNum; i++) {
                if (dim == i) continue;
                productOfCardinalitiesOfBlock *= (double) getTableSize(i, "B");
                if (productOfCardinalitiesOfBlock == 0) {
                    return -1;
                }
            }
            productOfCardinalitiesOfBlock *= (double) getTableSize("Bprime");
            if (productOfCardinalitiesOfBlock == 0) {
                return -1;
            }
            double tmp = productOfCardinalitiesOfBlock / productOfCardinalitiesOfAll;
            return MB * (Math.log(MB / MR) - 1) + MR * tmp - MB * Math.log(tmp);
        } else {
            System.err.println("Incorrect density measure!");
        }
        return 0;
    }

    public static boolean existAttributeValue(int i, String attributeValue) {
        Statement stmt = null;
        String sql = null;
        ResultSet rs = null;

        try {
            stmt = c.createStatement();

            // DROP table if exists previously
            sql = "SELECT * FROM B" + String.valueOf(i) + " WHERE col='" + attributeValue + "'";
            rs = stmt.executeQuery(sql);

            if (rs.next()) {
                return true;
            }
            stmt.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    public static void mainFunc() {
        System.out.println("=== DCUBE ===");

        Statement stmt = null;
        Statement localStmt = null;
        StringBuilder sql = null;
        String colNames = "";

        try {
            stmt = c.createStatement();
            localStmt = c.createStatement();

            // DROP table if exists previously
            sql = new StringBuilder("DROP TABLE IF EXISTS RORI");
            stmt.executeUpdate(sql.toString());

            // CREATE table RORI
            sql = new StringBuilder("CREATE TABLE RORI ( ");
            sql.append("col1" + " CHAR(100)");
            colNames = "(col1";
            for (int i = 2; i <= attributeNum; i++) {
                sql.append(", col").append(String.valueOf(i)).append(" CHAR(100)");
                colNames += ", col" + String.valueOf(i);
            }
            sql.append(", cnt INT )");
            colNames += ", cnt)";
            stmt.executeUpdate(sql.toString());
            System.out.println("Create RORI table done");

            // Create index on RORI
            sql = new StringBuilder("CREATE INDEX ON RORI " + colNames);
            stmt.executeUpdate(sql.toString());

            // Copy R -> RORI
            sql = new StringBuilder("INSERT INTO RORI SELECT " + colNames.substring(1, colNames.length()-1) + " FROM R");
            stmt.executeUpdate(sql.toString());
            System.out.println("Copy to RORI done");

            // Compute {Rn}
            for (int i = 1; i <= attributeNum; i++) {
                String tableName = "R" + String.valueOf(i);

                // DROP table if exists previously
                sql = new StringBuilder("DROP TABLE IF EXISTS " + tableName);
                stmt.executeUpdate(sql.toString());

                // Create
                sql = new StringBuilder("CREATE TABLE " + tableName + " ( col CHAR(100) )");
                stmt.executeUpdate(sql.toString());

                // Select
                sql = new StringBuilder("INSERT INTO " + tableName + " SELECT DISTINCT col" + String.valueOf(i) + " FROM R");
                stmt.executeUpdate(sql.toString());

                // Create index on {Rn}
                sql = new StringBuilder("CREATE INDEX ON " + tableName + " (col)");
                stmt.executeUpdate(sql.toString());
            }
            System.out.println("Compute {Rn} done");


            productOfCardinalitiesOfAll = 1.0;
            for (int i = 1; i <= attributeNum; i++) {
                productOfCardinalitiesOfAll *= (double) getTableSize(i, "R");
            }

            sql = new StringBuilder("DROP TABLE IF EXISTS DENSITY");
            stmt.executeUpdate(sql.toString());
            sql = new StringBuilder("CREATE TABLE DENSITY (d DOUBLE PRECISION)");
            stmt.executeUpdate(sql.toString());

            sql = new StringBuilder("DROP TABLE IF EXISTS BEST_DENSITY");
            stmt.executeUpdate(sql.toString());
            sql = new StringBuilder("CREATE TABLE BEST_DENSITY (d DOUBLE PRECISION)");
            stmt.executeUpdate(sql.toString());

            ResultSet rs = null;
            for (int i = 1; i <= k; i++) {

                // Result <- null
                // DROP table if exists previously
                sql = new StringBuilder("DROP TABLE IF EXISTS RESULTS").append(i);
                stmt.executeUpdate(sql.toString());
//
//                sql = new StringBuilder("CREATE TABLE RESULTS").append(i).append(" ( ");
//                sql.append("col1" + " CHAR(100)");
//                colNames = "(col1";
//                for (int j = 2; j <= attributeNum; j++) {
//                    sql.append(", col").append(String.valueOf(j)).append(" CHAR(100)");
//                    colNames += ", col" + String.valueOf(i);
//                }
//                sql.append(", cnt INTEGER");
//                sql.append(" )");
//                colNames += ")";
//                stmt.executeUpdate(sql.toString());
//                sql = new StringBuilder("CREATE INDEX ON RESULTS " + colNames);
//                stmt.executeUpdate(sql.toString());
//                System.out.println("Result <- null done");

                /* Compute MR */
                double MR = 0;
                sql = new StringBuilder("SELECT SUM(cnt) AS SUMCNT from R where del!=1");
                rs = stmt.executeQuery(sql.toString());
                rs.next();
                MR = (double) rs.getInt("SUMCNT");
                System.out.println("Compute MR done");

                /* find_single_block */
                double density = findSingleBlock(attributeNum, MR);

                /* Update R */
//                sql = new StringBuilder("DELETE FROM R WHERE (");
//                colNames = "";
//                for (int j = 1; j <= attributeNum; j++) {
//                    if (j != 1) colNames += " AND ";
//                    colNames += "col" + String.valueOf(j) + " IN (SELECT col FROM B" + String.valueOf(j) + ")";
//                }
//                sql.append(colNames);
//                sql.append(")");
//                stmt.executeUpdate(sql.toString());


                /* Update R: mark delete */
                sql = new StringBuilder("UPDATE R SET del = 1 WHERE (");
                colNames = "";
                for (int j = 1; j <= attributeNum; j++) {
                    if (j != 1) colNames += " AND ";
                    colNames += "col" + String.valueOf(j) + " IN (SELECT col FROM B" + String.valueOf(j) + ")";
                }
                sql.append(colNames);
                sql.append(")");
                stmt.executeUpdate(sql.toString());


//                sql = new StringBuilder("SELECT * from R");
//                rs = stmt.executeQuery(sql.toString());
//
//                // For each query
//                while (rs.next()) {
//                    int j = 1;
//                    String condition = "";
//
//                    // For each attribute
//                    for (j = 1; j <= attributeNum; j++) {
//                        String colName = "col" + String.valueOf(j);
//                        String attributeValue = rs.getString(colName);
//
//                        if (!existAttributeValue(j, attributeValue)) {
//                            break;
//                        }
//
//                        if (j != 1) condition += " AND ";
//                        condition += colName + "='" + attributeValue + "'";
//                    }
//
//                    // Delete the tuple in R
//                    if (j > attributeNum) {
//                        sql = new StringBuilder("DELETE FROM R WHERE " + condition);
//                        localStmt.executeUpdate(sql.toString());
//                    }
//                }

                /* Update RESULTS */
                System.err.println("SELECT * INTO RESULTS" + String.valueOf(i) + " FROM RORI WHERE (" + colNames + ")");
                sql = new StringBuilder("SELECT * INTO RESULTS" + String.valueOf(i) + " FROM RORI WHERE (" + colNames + ")");
                stmt.executeUpdate(sql.toString());
//                sql = new StringBuilder("SELECT * from RORI");
//                rs = stmt.executeQuery(sql.toString());
//
//                // For each query
//                while (rs.next()) {
//                    int j = 1;
//                    String values = "";
//                    String condition = "";
//
//                    // For each attribute
//                    for (j = 1; j <= attributeNum; j++) {
//                        String colName = "col" + String.valueOf(j);
//                        String attributeValue = rs.getString(colName);
//
//
//                        if (!existAttributeValue(j, attributeValue)) {
//                            break;
//                        }
//
//                        if (j != 1) {
//                            values += ", ";
//                            condition += " AND ";
//                        }
//                        values += "'" + attributeValue + "'";
//                        condition += colName + "='" + attributeValue + "'";
//                    }
//
//                    // Insert into RESULTS
//                    if (j > attributeNum) {
//                        j = rs.getInt("cnt");
//                        condition += " AND cnt=" + j;
//                        values += ", " + j;
//
//                        sql = new StringBuilder("INSERT INTO results").append(i).append(" ( col1");
//                        for (int p = 2; p <= attributeNum; p++) {
//                            sql.append(", col").append(String.valueOf(p));
//                        }
//                        sql.append(", cnt");
//                        sql.append(" ) SELECT ").append(values);
//                        sql.append(" WHERE NOT EXISTS (SELECT 1 FROM results").append(i).append(" WHERE ").append(condition).append(" )");
//                        localStmt.executeUpdate(sql.toString());
//
//                    }
//                }

                /////////////


//                // Compute {Rn}
//                for (int j = 1; j <= attributeNum; j++) {
//                    String tableName = "R" + String.valueOf(j);
//
//                    // DROP table if exists previously
//                    sql = new StringBuilder("DROP TABLE IF EXISTS " + tableName);
//                    stmt.executeUpdate(sql.toString());
//
//                    // Create
//                    sql = new StringBuilder("CREATE TABLE " + tableName + " ( col CHAR(100) )");
//                    stmt.executeUpdate(sql.toString());
//
//                    // Select
//                    sql = new StringBuilder("INSERT INTO " + tableName + " SELECT DISTINCT col" + String.valueOf(j) + " FROM R WHERE del=0");
//                    stmt.executeUpdate(sql.toString());
//
//                    // Create index on {Rn}
//                    sql = new StringBuilder("CREATE INDEX ON " + tableName + " (col)");
//                    stmt.executeUpdate(sql.toString());
//                }
//                System.out.println("Update {Rn} done");


//                // Calculate density
//                sql = new StringBuilder("SELECT SUM(cnt) AS SUMCNT from results").append(i);
//                rs = stmt.executeQuery(sql.toString());
//                rs.next();
//                double MB = (double) rs.getInt("SUMCNT");
////                localRs.close();
//
//                sql = new StringBuilder("SELECT SUM(cnt) AS SUMCNT from RORI");
//                rs = stmt.executeQuery(sql.toString());
//                rs.next();
//                double MRori = (double) rs.getInt("SUMCNT");
//
//                System.out.println("MB="+MB + ",MR="+MRori);
//
//                // Bn <- copy(Rn), for all n in [N]
//                for (int j = 1; j <= attributeNum; ++j) {
//                    sql = new StringBuilder("DROP TABLE IF EXISTS B").append(j);
//                    stmt.executeUpdate(sql.toString());
//                    sql = new StringBuilder("CREATE TABLE B").append(j).append(" ( col CHAR(100) )");
//                    stmt.executeUpdate(sql.toString());
//                    sql = new StringBuilder("CREATE INDEX ON B").append(j).append(" ( col )");
//                    stmt.executeUpdate(sql.toString());
//
//                    // Select
//                    sql = new StringBuilder("INSERT INTO B").append(j).append(" SELECT DISTINCT col").append(j).append(" FROM results").append(i);
//                    stmt.executeUpdate(sql.toString());
//                }

//                double density2 = densityMeasure(MB, MRori);
                System.out.println("block density: " + density);
//                System.out.println("block density2: " + density2);
                sql = new StringBuilder("INSERT INTO DENSITY VALUES (" + density + ")");
                stmt.executeUpdate(sql.toString());
//                sql = new StringBuilder("INSERT INTO BEST_DENSITY VALUES (" + density + ")");
//                stmt.executeUpdate(sql.toString());
            }
            rs.close();

            stmt.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Results are saved in database: results* and density");
    }


    public static double findSingleBlock(int N, double massR) {
        Statement stmt, localStmt;
        double massB;
        int i, j, r, r_tilde;
        String a;
        double rho_tilde, rho_prime, mb, threshold;
        int emptyBnNum = 0, size;

        System.out.println("=== find_single_block ===");

        try {

            System.out.println("Initializing...");

            // Clear cache
            cardinalities.clear();

            stmt = c.createStatement();
            localStmt = c.createStatement();

            // B <- copy(R)
            String sql = "DROP TABLE IF EXISTS B";
            stmt.executeUpdate(sql);
            sql = "CREATE TABLE B AS TABLE R";
            stmt.executeUpdate(sql);
            sql = "CREATE INDEX ON B " + colNames;
            stmt.executeUpdate(sql);
            sql = "DELETE FROM B WHERE del=1";
            stmt.executeUpdate(sql);
//            sql = "CREATE TABLE B (LIKE R INCLUDING INDEXES)";
//            stmt.executeUpdate(sql);
//            sql = "INSERT INTO B SELECT * FROM R";
//            stmt.executeUpdate(sql);

            // MB <- MR
            massB = massR;

            System.out.print("Cardinalities: (");
            // Bn <- copy(Rn), for all n in [N]
            for (i = 1; i <= N; ++i) {
                sql = "DROP TABLE IF EXISTS B" + i;
                stmt.executeUpdate(sql);
                sql = "CREATE TABLE B" + i + " as TABLE R" + i;
                stmt.executeUpdate(sql);
                sql = "CREATE INDEX ON B" + i + " (col)";
                stmt.executeUpdate(sql);
//                sql = "CREATE TABLE B" + i + " (LIKE R" + i + " INCLUDING INDEXES)";
//                stmt.executeUpdate(sql);
//                sql = "INSERT INTO B" + i + " SELECT * FROM R" + i;
//                stmt.executeUpdate(sql);

                // Warm up cache
                System.out.print(getTableSize(i, "B") + ",");
            }
            System.out.println("\b)");

            // max density so far
            rho_tilde = densityMeasure(massB, massR);

            // current order of attribute values
            r = 1;

            // order of max density entry
            r_tilde = 1;

            // Create table to store order number for each removed tuple
            sql = "DROP TABLE IF EXISTS orders";
            stmt.executeUpdate(sql);
            sql = "CREATE TABLE orders (a CHAR(100), i INTEGER, r INTEGER)";
            stmt.executeUpdate(sql);
            sql = "CREATE INDEX ON orders (a, i, r)";
            stmt.executeUpdate(sql);

            System.out.println("Start removing blocks...");

            File file = new File(TMP_FILE_PATH);
            file.delete();
            FileWriter fw = new FileWriter(TMP_FILE_PATH);

            // Until all are removed
            while (emptyBnNum < N) {

                sql = "DROP TABLE IF EXISTS MB";
                stmt.executeUpdate(sql);
                sql = "CREATE TABLE MB (a CHAR(100), i INTEGER, mass DOUBLE PRECISION)";
                stmt.executeUpdate(sql);
                sql = "CREATE INDEX ON MB (a, i, mass)";
                stmt.executeUpdate(sql);

                System.out.println("Computing the Attribute-Value Mass for all a in Bn (n in [N])");

//                t1 = System.currentTimeMillis();
                // Compute the Attribute-Value Mass for all a in Bn (n in [N])
                for (i = 1; i <= N; ++i) {

                    // Version 1
//                    long t3 = System.currentTimeMillis();
//                    sql = "SELECT * FROM B" + i;
//                    ResultSet rs = stmt.executeQuery(sql);
////                    long t4 = System.currentTimeMillis();
////                    System.out.println("Selected * from b: " + (t4-t3));
//                    while (rs.next()) {
//                        a = rs.getString("col");
//
////                        t3 = System.currentTimeMillis();
//                        sql = String.format("SELECT SUM(cnt) AS SUMCNT from B WHERE col%d='%s'", i, a);
//                        ResultSet localRs = localStmt.executeQuery(sql);
////                        t4 = System.currentTimeMillis();
////                        System.out.println("Get sum: " + (t4-t3));
//                        localRs.next();
//                        mb = (double) localRs.getInt("SUMCNT");
//
////                        t3 = System.currentTimeMillis();
//                        sql = String.format("INSERT INTO MB VALUES ('%s', %d, %f)", a, i, mb);
//                        localStmt.executeUpdate(sql);
////                        t4 = System.currentTimeMillis();
////                        System.out.println("Inserted: " + (t4-t3));
//                    }
//                    rs.close();

                    // Version 2
//                    sql = String.format("SELECT col%d AS col, SUM(cnt) AS SUMCNT from B GROUP BY col%d", i, i);
//                    ResultSet rs = stmt.executeQuery(sql);
////                    System.out.println("Selected from b: " + (t4-t3));
//                    while (rs.next()) {
//                        a = rs.getString("col");
//                        mb = (double) rs.getInt("SUMCNT");
//                        sql = String.format("INSERT INTO MB VALUES ('%s', %d, %f)", a, i, mb);
//                        localStmt.executeUpdate(sql);
//                    }
//                    rs.close();

                    // Version 3
                    sql = String.format("INSERT INTO MB (a, i, mass) " +
                            "(SELECT B%d.col AS a, %d as i, COALESCE(SUM(cnt), 0) AS mass " +
                            "FROM B%d LEFT OUTER JOIN B " +
                            "ON (B.col%d = B%d.col) " +
                            "GROUP BY B%d.col)", i, i, i, i, i, i);
                    stmt.executeUpdate(sql);
                }

                // Select dimension
                i = selectDimension(massB, massR);

                System.out.println("Selected dimension: " + i);

                System.out.println("Computing the low-mass tuples in ascending order");
                // Compute the low-mass tuples in ascending order
                sql = "DROP TABLE IF EXISTS Di";
                stmt.executeUpdate(sql);
                sql = "CREATE TABLE Di (a CHAR(100), mb DOUBLE PRECISION)";
                stmt.executeUpdate(sql);
                sql = "CREATE INDEX ON Di (a, mb)";
                stmt.executeUpdate(sql);

                size = getTableSize(i, "B");
                threshold = massB / size;

                System.out.printf("  MB=%s, |B%d|=%d, MB/|B%d|=%s%n", massB, i, size, i, threshold);

                sql = "INSERT INTO Di (" +
                        " SELECT a, mass FROM MB" +
                        " WHERE mass<=" + threshold +
                        " AND i=" + i +
                        " ORDER BY mass" +
                        " )";
                stmt.executeUpdate(sql);

                sql = "SELECT a, mb FROM Di";
                ResultSet rs = stmt.executeQuery(sql);
                int count = 0;
                StringBuilder orderBuffer = new StringBuilder();
                while (rs.next()) {
                    ++count;

                    int card = cardinalities.get("B" + i);
                    cardinalities.put("B" + i, card - 1);

                    a = rs.getString("a");

                    // MB <- MB - MB(a,i)
                    massB -= rs.getDouble("mb");

                    // Update density after a is removed
                    rho_prime = densityMeasure(massB, massR);

                    // Store the order for the tuple to be removed in buffer
                    orderBuffer.append(a).append(',')
                            .append(i).append(',')
                            .append(r).append("\n");

                    // r <- r + 1
                    ++r;

                    // Update max density so far
                    if (rho_prime > rho_tilde) {
//                        System.out.println("new max density: "+rho_prime + " at " + r);
                        rho_tilde = rho_prime;
                        r_tilde = r;
                    }
                }
                rs.close();

                System.out.println("Deleted " + count + " values from B" + i);

                sql = "DELETE FROM B" + i + " WHERE col IN (SELECT a FROM Di)";
                stmt.executeUpdate(sql);

                sql = "DELETE FROM B WHERE col" + i + " IN (SELECT a FROM Di)";
                localStmt.executeUpdate(sql);

                fw.write(orderBuffer.toString());

                // All possible values for a attribute are removed
                size = getTableSize(i, "B");
                if (size == 0) {
                    System.out.println("Table B" + i + " is empty");
                    ++emptyBnNum;
                } else {
                    System.out.println("Table B" + i + " remains " + size + " values");
                }
            }
            System.out.println("--maxIters=" + r_tilde);

            System.out.println("Reconstruct the dense block");

            cardinalities.clear();

            fw.flush();
            fw.close();

            sql = "COPY orders FROM '" + TMP_FILE_PATH + "' WITH DELIMITER ',' CSV";
            stmt.executeUpdate(sql);

            // Reconstruct the dense block
            for (i = 1; i <= N; ++i) {
                sql = "INSERT INTO B" + i + " (" +
                        " SELECT Ri.col as col FROM R" + i + " as Ri, orders" +
                        " WHERE orders.a=Ri.col" +
                        " AND orders.i=" + i +
                        " AND orders.r>=" + r_tilde +
                        " )";
                stmt.executeUpdate(sql);
            }
            for (i = 1; i <= N; ++i) {
                System.out.print(" " + getTableSize(i, "B") + ",");
            }
            System.out.println();

            localStmt.close();
            stmt.close();

            System.out.println("=== find_single_block: " + rho_tilde + "===");

            return rho_tilde;
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
        return 0;
    }

    private static int selectDimension(double... mass) {
        switch (selectDimension) {
            case "cardinality":
                return selectDimensionByCardinality();
            case "density":
                try {
                    return selectDimensionByDensity(mass[0], mass[1]);
                } catch (SQLException e) {
                    e.printStackTrace();
                    return -1;
                }
            default:
                System.err.println("Unsupported dimension selection method.");
                return -1;
        }
    }

    public static void main(String[] args) {
        long t1 = System.currentTimeMillis();
        // Input format: java DCube example_data.txt output 3 geo density 3
        String fileInputPath = args[0];
        String fileOutputPath = args[1];
        DCube.attributeNum = Integer.valueOf(args[2]);
        DCube.denMeature = args[3];
        DCube.selectDimension = args[4];
        DCube.k = Integer.valueOf(args[5]);


        try {
            TMP_FILE_PATH = new File("orders.csv").getCanonicalPath();
            String port = ENV.get("PGPORT");
            String user = ENV.get("USER");
            DCube.c = openDatabase(port, user, user);
            importData(fileInputPath);

            mainFunc();

            DCube.c.close();
            System.out.println("Closed database successfully");
        } catch (Exception e) {
            e.printStackTrace();
        }
        long t2 = System.currentTimeMillis();
        System.out.println("Time elasped: " + (t2 - t1) + " ms");
    }
}
