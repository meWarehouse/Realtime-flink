package com.at.retime.app.func;

import com.at.retime.utils.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * @author zero
 * @create 2021-05-23 19:23
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class KeywordUDTF extends TableFunction<Row> {

    public void eval(String str) {

        List<String> list = KeywordUtil.analyze(str);

        for (String s : list) {
//            collect(Row.of(s));
            Row row = new Row(1);
            row.setField(0,s);
            collect(row);
        }

//        for (String s : str.split(" ")) {
//            // use collect(...) to emit a row
//            collect(Row.of(s));
//        }
    }
}
