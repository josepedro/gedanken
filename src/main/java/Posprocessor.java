import java.io.*;
import java.util.*;

public class Posprocessor {

    private String fileInputPath;
    private String fileOutputPath;

    public Posprocessor(String fileInputPath, String fileOutputPath) {
        this.fileInputPath = fileInputPath;
        this.fileOutputPath = fileOutputPath;
    }

    public void posprocess(){

        // This will reference one line at a time
        String line = null;
        try {
            // FileReader reads text files in the default encoding.
            FileReader fileReader =
                    new FileReader(fileInputPath);

            PrintWriter writer = new PrintWriter(fileOutputPath, "UTF-8");

            // Always wrap FileReader in BufferedReader.
            BufferedReader bufferedReader =
                    new BufferedReader(fileReader);

            int id = 0;
            Map<Integer, Double> densityMatrix = new HashMap<>();
            while((line = bufferedReader.readLine()) != null) {
                String[] cell = line.split(" ");
                densityMatrix.put(Integer.parseInt(cell[0]), Double.parseDouble(cell[1]));
            }

            Map<Integer, Double> treeMapDensity = new TreeMap<Integer, Double>(densityMatrix);

            Set s = treeMapDensity.entrySet();
            Iterator it = s.iterator();
            while ( it.hasNext() ) {
                Map.Entry entry = (Map.Entry) it.next();
                Integer key = (Integer) entry.getKey();
                System.out.println("Ordem");
                System.out.println(key);
                Double value = (Double) entry.getValue();
                writer.print(value);
                writer.print(" ");
                //System.out.println(key + " => " + value);
            }

            // Always close files.
            bufferedReader.close();
            writer.close();
        }
        catch(FileNotFoundException ex) {
            System.out.println(
                    "Unable to open file");
        }
        catch(IOException ex) {
            System.out.println(
                    "Error reading file");
            // Or we could just do this:
            // ex.printStackTrace();
        }

    }
}
