package net.thornydev;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.Set;
import java.util.HashSet;

import java.awt.datatransfer.StringSelection;
import java.awt.Toolkit;
import java.awt.datatransfer.Clipboard;

import com.sun.org.apache.xpath.internal.functions.FuncFalse;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;



/**
 * Generates Hive schemas for use with the JSON SerDe from
 * org.openx.data.jsonserde.JsonSerDe.  GitHub link: https://github.com/rcongiu/Hive-JSON-Serde
 * 
 * Pass in a valid JSON document string to {@link JsonHiveSchema#createHiveSchema} and it will
 * return a Hive schema for the JSON document.
 * 
 * It supports embedded JSON objects, arrays and the standard JSON scalar types: strings,
 * numbers, booleans and null.  You probably don't want null in the JSON document you provide
 * as Hive can't use that.  For numbers - if the example value has a decimal, it will be 
 * typed as "double".  If the number has no decimal, it will be typed as "int".
 * 
 * This program uses the JSON parsing code from json.org and that code is included in this
 * library, since it has not been packaged and made available for maven/ivy/gradle dependency
 * resolution.
 * 
 * <strong>Use of main method:</strong> <br>
 *   JsonHiveSchema has a main method that takes a file path to a JSON doc - this file should have
 *   only one JSON file in it.  An optional second argument can be provided to name the Hive table
 *   that is generated.
 */
public class JsonHiveSchema {

  static void help() {
    System.out.println("Usage: Two arguments possible. First is required. Second is optional");
    System.out.println("  1st arg: path to JSON file to parse into Hive schema");
    System.out.println("  2nd arg (optional): tablename.  Defaults to 'x'");
    System.out.println("  3rd arg (optional): s3_path.  Defaults to 'y'");
    System.out.println("  4th arg (optional): serdeProperties flag  Defaults to False");
  }

  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      throw new IllegalArgumentException("ERROR: No file specified");
    }

    if (args[0].equals("-h")) {
      help();
      System.exit(0);
    }

    StringBuilder sb = new StringBuilder();
    BufferedReader br = new BufferedReader(new FileReader(args[0]));
    String line;
    while ((line = br.readLine()) != null) {
      sb.append(line).append("\n");
    }
    br.close();

    String tableName = "x";
    String s3Path = "y";
    boolean serdePropertiesFlag = false;

    JsonHiveSchema schemaWriter = null;
    if (args.length == 2) {
      tableName = args[1];
      schemaWriter = new JsonHiveSchema(tableName);
    } else if (args.length == 3) {
      tableName = args[1];
      s3Path = args[2];
      schemaWriter = new JsonHiveSchema(tableName, s3Path, serdePropertiesFlag);
    }
    StringSelection stringSelection = new StringSelection(schemaWriter.createHiveSchema(sb.toString()));
    Clipboard clipboard = Toolkit.getDefaultToolkit().getSystemClipboard();
    clipboard.setContents(stringSelection, null);

//    System.out.println(schemaWriter.createHiveSchema(sb.toString()));
  }


  private String tableName = "x";
  private String s3Path = "y";
  private boolean serdePropertiesFlag = false;


  public JsonHiveSchema() {
  }

  public JsonHiveSchema(String tableName) {
    this.tableName = tableName;
  }

  public JsonHiveSchema(String tableName, String s3Path, boolean serdePropertiesFlag) {
    this.tableName = tableName;
    this.s3Path = s3Path;
    this.serdePropertiesFlag = serdePropertiesFlag;
  }

  /**
   * Pass in any valid JSON object and a Hive schema will be returned for it.
   * You should avoid having null values in the JSON document, however.
   * <p>
   * The Hive schema columns will be printed in alphabetical order - overall and
   * within subsections.
   *
   * @param json
   * @return string Hive schema
   * @throws JSONException if the JSON does not parse correctly
   */
  public String createHiveSchema(String json) throws JSONException {
    JSONObject jo = new JSONObject(json);

    @SuppressWarnings("unchecked")
    Iterator<String> keys = jo.keys();
    keys = new OrderedIterator(keys);
    StringBuilder sb = new StringBuilder("DROP TABLE IF EXISTS ").append(tableName).append(";\n");
    sb.append("CREATE external TABLE IF NOT EXISTS " + tableName + " (\n");
    StringBuilder serdeProperties = new StringBuilder();

    while (keys.hasNext()) {
      String k = keys.next();
      sb.append("  ");
      sb.append(k.toString());
      sb.append(' ');
      sb.append(valueToHiveSchema(jo.opt(k)));
      sb.append(',').append("\n");
    }
    StringBuilder builder = new StringBuilder();
    if (serdePropertiesFlag) {
      serdeProperties.append(toCreateSerdePropertyMapping(jo));
      Set<String> s = new HashSet<String>();
      String[] st = serdeProperties.toString().split(",");
      for (String str : st) {
        s.add(str);
      }

      for (String str : s) {
        if (str.trim() != "") {
          builder.append(str + ",");
        }
      }
    }


    sb.replace(sb.length() - 2, sb.length(), ")\n"); // remove last comma
//    sb.append("PARTITIONED BY (schd_date string,hour string)\n");
    sb.append("ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'");
    sb.append("\n");
    if (serdePropertiesFlag){
      sb.append("WITH SERDEPROPERTIES (");
      builder.replace(builder.length() - 1, builder.length(), ")");
      sb.append(builder.toString());
    }
    sb.append("Location '");
    sb.append(s3Path);
    return sb.append("';").toString();
  }

  private String toCreateSerdePropertyMapping(Object o) throws JSONException {
    if (o instanceof JSONObject) {
      return toSerdePropertiesString((JSONObject)o);
    } else if(o instanceof JSONArray) {
      return toSerdePropertiesString((JSONArray)o);
    } else {
      return "";
    }
}


  private String replaceDashWithUnderscore(String value) {
    String newValue = value.replace("-", "_");
    return newValue;
  }

  private String toSerdePropertiesString(JSONObject jo) throws JSONException {
    Iterator<String> keys = jo.keys();
    keys = new OrderedIterator(keys);
    StringBuilder sb = new StringBuilder();

    while (keys.hasNext()) {
      String k = keys.next();
      if (k.indexOf("-") > 0) {
        sb.append("'mapping." + replaceDashWithUnderscore(k.toString()) + "'='" +  k.toString() + "',");
        sb.append("\n");
      }
      sb.append(toCreateSerdePropertyMapping(jo.opt(k)));
    }
    return sb.toString();
  }


  private String toSerdePropertiesString(JSONArray a) throws JSONException {
    StringBuilder sb = new StringBuilder();
    JSONArray ja = new JSONArray(a.toString());

    if (ja.length() == 0) {
      throw new IllegalStateException("Array is empty: " + ja.toString());
    }
    Object entry0 = ja.get(0);
    if (entry0.toString().indexOf( '{') == -1) return "";
    JSONObject jo = new JSONObject(entry0.toString());
    Iterator<String> keys = jo.keys();
    keys = new OrderedIterator(keys);
    while(keys.hasNext()) {
      String k = keys.next();
      if (k.indexOf("-") > 0) {
        sb.append("'mapping." + replaceDashWithUnderscore(k.toString()) + "'='" +  k.toString() + "',");
        sb.append("\n");
      }
      sb.append(toCreateSerdePropertyMapping(jo.opt(k)));
    }
    return sb.toString();
  }




  private String toHiveSchema(JSONObject o) throws JSONException { 
    @SuppressWarnings("unchecked")
    Iterator<String> keys = o.keys();
    keys = new OrderedIterator(keys);

    if (o.length() == 0) return "string";

    StringBuilder sb = new StringBuilder("struct<");
    
    while (keys.hasNext()) {
      String k = keys.next();
      if (keyWord(k)){
        sb.append(replaceDashWithUnderscore("`" + k.toString() + "`"));
      } else {
        sb.append(replaceDashWithUnderscore(k.toString()));
      }
      sb.append(':');
      sb.append(valueToHiveSchema(o.opt(k)));
      sb.append(", ");
    }

//    if (keys.hasNext()){
//      System.out.println("Print Key: " + o.toString());
//    }
    sb.replace(sb.length() - 2, sb.length(), ">"); // remove last comma
    return sb.toString();
  }

  private boolean keyWord(String k) {
    String regex = "\\d+";
    if (k.equals("end") || k.equals("bucket") || k.matches(regex)){
      return true;
    }
    return false;
  }


  private String toHiveSchema(JSONArray a) throws JSONException {
    return "array<" + arrayJoin(a, ",") + '>';
  }

  private String arrayJoin(JSONArray a, String separator) throws JSONException {
    StringBuilder sb = new StringBuilder();

    if (a.length() == 0) {
//      throw new IllegalStateException("Array is empty: " + a.toString());
      return "array<string>";
    }
    
    Object entry0 = a.get(0);
    if ( isScalar(entry0) ) {
      sb.append( scalarType(entry0) );
    } else if (entry0 instanceof JSONObject) {
      sb.append( toHiveSchema((JSONObject)entry0) );
    } else if (entry0 instanceof JSONArray) {    
      sb.append( toHiveSchema((JSONArray)entry0) );
    }
    return sb.toString();
  }
  
  private String scalarType(Object o) {
    if (o instanceof String) return "string";
    if (o instanceof Number) return scalarNumericType(o);
    if (o instanceof String[]) return "array<string>";
    if (o instanceof Boolean) return "boolean";
    if (o == JSONObject.NULL) return "string";
    return null;
  }

  private String scalarNumericType(Object o) {
    String s = o.toString();
    if (s.indexOf('.') > 0) {
      return "double";
    } else {
      return "int";
    }
  }

  private boolean isScalar(Object o) {
    return o instanceof String ||
        o instanceof Number ||
        o instanceof Boolean || o instanceof String[] ||
        o == JSONObject.NULL;
  }

  private String valueToHiveSchema(Object o) throws JSONException {
    if ( isScalar(o) ) {
      return scalarType(o);
    } else if (o instanceof JSONObject) {
      return toHiveSchema((JSONObject)o);
    } else if (o instanceof JSONArray) {
      return toHiveSchema((JSONArray)o);
    } else {
      throw new IllegalArgumentException("unknown type: " + o.getClass());
    }
  }



  static class OrderedIterator implements Iterator<String> {

    Iterator<String> it;
    
    public OrderedIterator(Iterator<String> iter) {
      SortedSet<String> keys = new TreeSet<String>();
      while (iter.hasNext()) {
        keys.add(iter.next());
      }
      it = keys.iterator();
    }
    
    public boolean hasNext() {
      return it.hasNext();
    }

    public String next() {
      return it.next();
    }

    public void remove() {
      it.remove();
    }
  }
}
