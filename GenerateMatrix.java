import java.util.Random;

import java.io.*;
import java.util.*;
import java.net.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class GenerateMatrix {
  public static void main(String[] args) {
    int m_i = Integer.parseInt(args[0]);
    int j = Integer.parseInt(args[1]);
    int n_k = Integer.parseInt(args[2]);
    
    makeMatrix(0, m_i, j);
    makeMatrix(1, j, n_k);
  }

  public static void makeMatrix(int mNum, int rows, int cols) {
    String mn_s = Integer.toString(mNum);

    try {
      Path file = new Path("hdfs://localhost:9000/user/hduser/input/file" + mn_s);
      FileSystem fs = FileSystem.get(new Configuration());

      BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fs.create(file,true)));
      Random rand = new Random();

      for (int i = 0; i < rows; ++i) {
        String row_s = Integer.toString(i);
        for (int j = 0; j <cols; ++j) {
          String col_s = Integer.toString(j); 
          int v = rand.nextInt(10);  
          String v_s = Integer.toString(v);

          String total = mn_s + "," + row_s + "," + col_s + "," + v_s;
          br.write(total, 0, total.length());
          br.newLine();
        }
      }
      br.close();
    } catch(Exception e) {
      //
    }
  }
}
