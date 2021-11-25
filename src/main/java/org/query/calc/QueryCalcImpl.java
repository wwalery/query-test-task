package org.query.calc;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

public class QueryCalcImpl implements QueryCalc {

  @Override
  public void select(Path t1, Path t2, Path t3, Path output) throws IOException {
    // - t1 is a file contains table "t1" with two columns "a" and "x". First line is a number of rows, then each
    //  line contains exactly one row, that contains two numbers parsable by Double.parse(): value for column a and
    //  x respectively.See test resources for examples.
    // - t2 is a file contains table "t2" with columns "b" and "y". Same format.
    // - t3 is a file contains table "t3" with columns "c" and "z". Same format.
    // - output is table stored in the same format: first line is a number of rows, then each line is one row that
    //  contains two numbers: value for column a and s.
    //
    // Number of rows of all three tables lays in range [0, 1_000_000].
    // It's guaranteed that full content of all three tables fits into RAM.
    // It's guaranteed that full outer join of at least one pair (t1xt2 or t2xt3 or t1xt3) of tables can fit into RAM.
    //
    // TODO: Implement following query, put a reasonable effort into making it efficient from perspective of
    //  computation time, memory usage and resource utilization (in that exact order). You are free to use any lib
    //  from a maven central.
    //
    // SELECT a, SUM(x * y * z) AS s FROM 
    // t1 LEFT JOIN (SELECT * FROM t2 JOIN t3) AS t
    // ON a < b + c
    // GROUP BY a
    // STABLE ORDER BY s DESC
    // LIMIT 10;
    // 
    // Note: STABLE is not a standard SQL command. It means that you should preserve the original order. 
    // In this context it means, that in case of tie on s-value you should prefer value of a, with a lower row number.
    // In case multiple occurrences, you may assume that group has a row number of the first occurrence.

// reads and group t1 (SELECT a FROM t1 + GROUP BY a) - for reduce computations later
    AtomicInteger counter = new AtomicInteger(0);
    Map<Double, T1Row> t1Map = new HashMap<>();
    try (Stream<String> t1stream = Files.lines(t1)) {

// there are no conditions for the number of lines in the file is differ from the number in the first line + 1       
// if not so, we need to read the first element from the stream, and then set limit stream to it
      t1stream.skip(1).forEach(line -> {
        Row row = new Row(line);
        T1Row t2Row = new T1Row(counter.incrementAndGet(), row.getCol1(), row.getCol2(), 0);
        t1Map.merge(row.getCol1(), t2Row, (oldRow, newRow) -> oldRow.addColValue(newRow.getCol2()));
      });
    }

// reads t2 (SELECT * FROM t2)
    List<Row> t2List;
    try (Stream<String> t2stream = Files.lines(t2)) {
      t2List = t2stream.skip(1).map(Row::new).collect(Collectors.toList());
    }

// reads t3 and make sum at the same time (for reduce memory usage) (SUM(x * y * z) AS s + JOIN t3)
    try (Stream<String> t3stream = Files.lines(t3)) {
      t3stream.skip(1).forEach(line -> {
        Row t3row = new Row(line);
        for (Map.Entry<Double, T1Row> t1Entry : t1Map.entrySet()) {
          final double a = t1Entry.getKey();
          for (Row t2row : t2List) {
            if (a < t3row.getCol1() + t2row.getCol1()) { // ON a < b + c
              t1Entry.getValue().addSum(t2row.getCol2() * t3row.getCol2());
            }
          }
        }
      });
    }

    List<String> immutableResult = t1Map.values().stream()
        .sorted() // STABLE ORDER BY s DESC
        .limit(10L) // LIMIT 10
        .map(row -> Double.toString(row.getCol1()) + ' ' + Double.toString(row.getSum()))
        .collect(Collectors.toList());

    ArrayList<String> result = new ArrayList<>(immutableResult);
    result.add(0, Integer.toString(immutableResult.size()));
    Files.write(output, result);
  }

  @Getter
  @AllArgsConstructor
  public static class Row {

    private double col1;
    private double col2;

    public Row(final String line) {
// StringUtils.split usualy consumes less resources, than String.split
      String[] cols = StringUtils.split(line, ' ');
      this.col1 = Double.parseDouble(cols[0]);
      this.col2 = Double.parseDouble(cols[1]);
    }

  }

}
