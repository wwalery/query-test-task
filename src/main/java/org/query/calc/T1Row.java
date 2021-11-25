package org.query.calc;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class T1Row implements Comparable<T1Row> {
  
  private final int row;
  private double col1;
  private double col2;
  private double sum;

  public T1Row addColValue(double value) {
    col2 += value;
    return this;
  }

  public T1Row addSum(double value) {
    sum += col2 * value;
    return this;
  }

  @Override
  public int compareTo(T1Row other) {
    return sum == other.getSum() 
        ? Integer.compare(row, other.getRow()) 
        : Double.compare(other.getSum(), sum);
  }
  
}
