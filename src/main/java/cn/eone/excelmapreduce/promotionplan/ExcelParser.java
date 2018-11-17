package cn.eone.excelmapreduce.promotionplan;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ExcelParser {
    private static final Log logger = LogFactory.getLog(ExcelParser.class);

    /**
     * 解析is
     *
     * @param is 数据源
     * @return String[]
     */
    public static String[] parseExcelData(InputStream is) {
        // 结果集
        List<String> resultList = new ArrayList<String>();

        try {
            // 获取Workbook
            HSSFWorkbook workbook = new HSSFWorkbook(is);
            // 获取sheet
            HSSFSheet sheet = workbook.getSheetAt(0);

            Iterator<Row> rowIterator = sheet.iterator();
            int i = 0;
            String creativeName = "";
            while (rowIterator.hasNext()) {
                Row row = rowIterator.next();
                i++;
                if (i == 3) {
                    StringBuilder rowString = new StringBuilder();
                    Iterator<Cell> colIterator = row.cellIterator();
                    int j = 0;
                    while (colIterator.hasNext()) {
                        Cell cell = colIterator.next();
                        j++;
                        if (j == 1) {
                            String[] split = cell.getStringCellValue().trim().split("\\(");
                            rowString.append(split[1].split("\\)")[0] + ",");
                            rowString.append(split[0] + ",");
                        }
                        if (j == 3) {
                            String[] split = cell.getStringCellValue().trim().split("\\~");
                            rowString.append(split[0]+",");
                            rowString.append(split[1]+",");
                        }
                        if (j == 5) {
                            rowString.append(cell.getStringCellValue().trim());
                        }

                    }
                    resultList.add(rowString.toString());
                }
            }
        } catch (IOException e) {
            logger.error("IO Exception : File not found " + e);
        }
        return resultList.toArray(new String[0]);
    }

}



