import java.io.File;
import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Scanner;

/**
 * Created by Jack
 */
public class Migration {

    private static final String DB_URL = "jdbc:oracle:thin:@dario.cs.uwec.edu:1521:csdev";

    private static final String username = "NORDELJR2317";
    private static final String password = "5PKNI61A";

    private Connection connection;
    private PreparedStatement pState;

    public Migration() throws FileNotFoundException{



        System.out.println("Hello World!");
        File data = new File("C:\\Users\\Jack\\Desktop\\CS260\\HW2\\FirstFive.txt");

        Scanner scan = new Scanner(data);
        scan.useDelimiter("\\|");
        while(scan.hasNext()){
            String a = scan.next();
            if(!(a.equals(""))) {
                System.out.print(a + "   ");
            }else{
                System.out.print("null   ");
            }
        }


    }

    public static void main(String[] args) throws FileNotFoundException{
        new Migration();
    }
}
