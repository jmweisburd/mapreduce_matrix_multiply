import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class IndexTuple implements WritableComparable {

  public IntWritable row, col;

  public IndexTuple() {
    this.row = new IntWritable();
    this.col = new IntWritable();
  }

  public IndexTuple(IntWritable r, IntWritable c) {
    this.row = new IntWritable(r.get());
    this.col = new IntWritable(c.get());
  }

  public IndexTuple(int r, int c) {
    this.row = new IntWritable(r);
    this.col = new IntWritable(c);
  }

  public void setAll(int r, int c) {
    this.row.set(r);
    this.col.set(c);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    row.readFields(in);
    col.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    row.write(out);
    col.write(out);
  }

  @Override
  public String toString() {
    return Integer.toString(row.get()) + "," + Integer.toString(col.get()) + "\t";
  }

  @Override
  public int compareTo(Object o) {
    IndexTuple it = (IndexTuple) o;
    if (this.row.compareTo(it.row) == 0) {
      return this.col.compareTo(it.col);
    }
    else {
      return (this.row.compareTo(it.row));
    }
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof IndexTuple) {
      IndexTuple oth = (IndexTuple) o;
      return ((this.row.get() == oth.row.get()) && (this.col.get() == oth.col.get()));
    } else {
      return false;
    }
  }
}
