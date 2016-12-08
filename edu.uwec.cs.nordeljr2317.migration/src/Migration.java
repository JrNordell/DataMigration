import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.*;

/**
 * Created by Jack
 */
public class Migration {

    private static final String DB_URL = "jdbc:oracle:thin:@dario.cs.uwec.edu:1521:csdev";

    private static final String username = "NORDELJR2317";
    private static final String password = "5PKNI61A";





    private Migration() throws java.io.IOException{
        Connection connection;
        ResultSet resultSet = null;
        String sql = "";
        PreparedStatement pState;

        BufferedReader in = new BufferedReader(new FileReader("C:\\Users\\Jack\\Desktop\\CS260\\HW2\\WyomingData.txt"));
        try {
            connection = DriverManager.getConnection(DB_URL, username, password);
            sql = "insert into h_big\n" +
                    "values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            pState = connection.prepareStatement(sql);
            String hi = in.readLine();
            while (in.ready()) {
                hi = in.readLine();
                String[] stringArr = hi.split("\\|");
                for (int i = 0; i < stringArr.length; i++) {
                    if ((stringArr[i].equals(""))) {
                        stringArr[i] = "0";
                    }
                }
                if (stringArr.length == 19) {
                    pState.setDouble(1, Double.parseDouble(stringArr[0]));
                    pState.setString(2, stringArr[1]);
                    pState.setString(3, stringArr[2]);
                    pState.setString(4, stringArr[3]);
                    pState.setString(5, stringArr[4]);
                    pState.setString(6, stringArr[5]);
                    pState.setString(7, stringArr[6]);
                    pState.setString(8, stringArr[7]);
                    pState.setString(9, stringArr[8]);
                    pState.setDouble(10, Double.parseDouble(stringArr[9]));
                    pState.setDouble(11, Double.parseDouble(stringArr[10]));
                    pState.setString(12, stringArr[11]);
                    pState.setString(13, stringArr[12]);
                    pState.setDouble(14, Double.parseDouble(stringArr[13]));
                    pState.setDouble(15, Double.parseDouble(stringArr[14]));
                    pState.setDouble(16, Double.parseDouble(stringArr[15]));
                    pState.setDouble(17, Double.parseDouble(stringArr[16]));
                    pState.setString(18, stringArr[17]);
                    pState.setString(19, stringArr[18]);
                    pState.setString(20, "");
                    pState.addBatch();
                } else {
                    pState.setString(1, stringArr[0]);
                    pState.setString(2, stringArr[1]);
                    pState.setString(3, stringArr[2]);
                    pState.setString(4, stringArr[3]);
                    pState.setString(5, stringArr[4]);
                    pState.setString(6, stringArr[5]);
                    pState.setString(7, stringArr[6]);
                    pState.setString(8, stringArr[7]);
                    pState.setString(9, stringArr[8]);
                    pState.setDouble(10, Double.parseDouble(stringArr[9]));
                    pState.setDouble(11, Double.parseDouble(stringArr[10]));
                    pState.setString(12, stringArr[11]);
                    pState.setString(13, stringArr[12]);
                    pState.setDouble(14, Double.parseDouble(stringArr[13]));
                    pState.setDouble(15, Double.parseDouble(stringArr[14]));
                    pState.setDouble(16, Double.parseDouble(stringArr[15]));
                    pState.setDouble(17, Double.parseDouble(stringArr[16]));
                    pState.setString(18, stringArr[17]);
                    pState.setString(19, stringArr[18]);
                    pState.setString(20, stringArr[19]);
                    pState.addBatch();
                }
            }
                pState.executeBatch();

                pState.clearBatch();



                sql = "Select unique(feature_class) from h_big";
                pState = connection.prepareStatement(sql);
                resultSet = pState.executeQuery();

                sql = "insert into h_feature_class_table " +
                        "values(?, ?)";
                pState = connection.prepareStatement(sql);
                int w = 1;
                while(resultSet.next()) {
                    pState.setInt(1,w);
                    pState.setString(2, resultSet.getString(1));
                    w++;
                    pState.addBatch();
                }
                pState.executeBatch();



            sql = "Select unique(state_numeric), state_alpha from h_big";
            pState = connection.prepareStatement(sql);
            resultSet = pState.executeQuery();

            sql = "insert into h_state_table " +
                    "values(?, ?)";
            pState = connection.prepareStatement(sql);
            while(resultSet.next()) {
                pState.setString(1, resultSet.getString(1));
                pState.setString(2, resultSet.getString(2));
                pState.addBatch();
            }
            pState.executeBatch();



            sql = "Select unique(county_name), county_numeric, state_numeric from h_big";
            pState = connection.prepareStatement(sql);
            resultSet = pState.executeQuery();

            sql = "insert into h_county_table " +
                    "values(?, ?, ?)";
            pState = connection.prepareStatement(sql);
            while(resultSet.next()) {
                pState.setString(1,resultSet.getString(1));
                pState.setString(2, resultSet.getString(2));
                pState.setString(3, resultSet.getString(3));
                pState.addBatch();
            }
            pState.executeBatch();



            sql = "Select unique(map_name) from h_big";
            pState = connection.prepareStatement(sql);
            resultSet = pState.executeQuery();

            sql = "insert into h_map_table " +
                    "values(?, ?)";
            pState = connection.prepareStatement(sql);
            int x = 1;
            while(resultSet.next()) {
                pState.setInt(1, x);
                pState.setString(2, resultSet.getString(1));
                x++;
                pState.addBatch();
            }
            pState.executeBatch();




            sql = "select unique(feature_id), feature_name, feature_class_id, h_big.state_numeric, county_name, " +
                    "prim_lat_dms, prim_long_dms, prim_lat_dec, prim_long_dec, source_lat_dms, source_long_dms, " +
                    "source_lat_dec, source_long_dec, elev_in_m, elev_in_ft, map_id, date_created, date_edited " +
            "from h_big " +
            "join h_feature_class_table on h_big.feature_class = h_feature_class_table.feature_class " +
            "join h_state_table on h_big.state_numeric = h_state_table.state_numeric " +
            "join h_map_table on h_big.map_name = h_map_table.map_name";
            pState = connection.prepareStatement(sql);
            resultSet = pState.executeQuery();

            sql = "insert into h_feature_table " +
                    "values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            pState = connection.prepareStatement(sql);
            while(resultSet.next()) {
                pState.setDouble(1, resultSet.getDouble(1));
                pState.setString(2, resultSet.getString(2));
                pState.setDouble(3, resultSet.getDouble(3));
                pState.setString(4, resultSet.getString(4));
                pState.setString(5, resultSet.getString(5));
                pState.setString(6, resultSet.getString(6));
                pState.setString(7, resultSet.getString(7));
                pState.setDouble(8, resultSet.getDouble(8));
                pState.setDouble(9, resultSet.getDouble(9));
                pState.setString(10, resultSet.getString(10));
                pState.setString(11, resultSet.getString(11));
                pState.setDouble(12, resultSet.getDouble(12));
                pState.setDouble(13, resultSet.getDouble(13));
                pState.setDouble(14, resultSet.getDouble(14));
                pState.setDouble(15, resultSet.getDouble(15));
                pState.setDouble(16, resultSet.getDouble(16));
                pState.setString(17, resultSet.getString(17));
                pState.setString(18, resultSet.getString(18));
                pState.addBatch();
            }
            pState.executeBatch();


            }catch(SQLException ex){
                ex.printStackTrace();
            }

        }


    public static void main(String[] args) throws java.io.IOException{
        new Migration();
    }
}
