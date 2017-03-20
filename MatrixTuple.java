import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class MatrixTuple implements Writable{

  public IntWritable matrixNum, oppNum, value;

  public MatrixTuple() {
    this.matrixNum = new IntWritable();
    this.oppNum = new IntWritable();
    this.value = new IntWritable();
  }

  public MatrixTuple(MatrixTuple mt) {
    this.matrixNum = new IntWritable(mt.matrixNum.get());
    this.oppNum = new IntWritable(mt.oppNum.get());
    this.value = new IntWritable(mt.value.get());
  }

  public MatrixTuple(int mn, int on, int v) {
    this.matrixNum = new IntWritable(mn);
    this.oppNum = new IntWritable(on);
    this.value = new IntWritable(v);
  }

  public void setAll(int mn, int on, int v) {
    this.matrixNum.set(mn);
    this.oppNum.set(on);
    this.value.set(v);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    matrixNum.readFields(in);
    oppNum.readFields(in);
    value.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    matrixNum.write(out);
    oppNum.write(out);
    value.write(out);
  }
}
