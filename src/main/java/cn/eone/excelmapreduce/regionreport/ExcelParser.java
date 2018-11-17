package cn.eone.excelmapreduce.regionreport;

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
            HSSFSheet sheet = workbook.getSheetAt(5);

            Iterator<Row> rowIterator = sheet.iterator();
            int i = 0;
            int j = 0;
            String id = "";
            String creativeName = "";
            while (rowIterator.hasNext()) {
                // 行
                Row row = rowIterator.next();
                // 字符串
                i++;
                if (i == 3) {
                    id = row.cellIterator().next().getStringCellValue().trim().split("\\(")[1].split("\\)")[0];
                }
                if (i > 6) {
                    StringBuilder rowString = new StringBuilder();
                    Iterator<Cell> colIterator = row.cellIterator();
                     j = 0;
                    while (colIterator.hasNext()) {
                        Cell cell = colIterator.next();
                        j++;
                        if (j == 1 && !cell.getStringCellValue().trim().equals("")) {
                            creativeName = cell.getStringCellValue().trim();
                        }
                        if (colIterator.hasNext()) {
                            if (j == 1) {
                                rowString.append(creativeName + ",");
                            }else {
                                rowString.append(cell.getStringCellValue().trim()+",");
                            }
                        } else {
                            rowString.append(cell.getStringCellValue().trim());
                        }
                    }
                    resultList.add(id + "," + rowString.toString());
                }
            }
        } catch (IOException e) {
            logger.error("IO Exception : File not found " + e);
        }
        return resultList.toArray(new String[0]);
    }

}
