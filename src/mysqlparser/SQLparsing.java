package mysqlparser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;

import org.gibello.zql.ZExpression;
import org.gibello.zql.ZFromItem;
import org.gibello.zql.ZInsert;
import org.gibello.zql.ZQuery;
import org.gibello.zql.ZSelectItem;
import org.gibello.zql.ZStatement;
import org.gibello.zql.ZqlParser;
import org.gibello.zql.data.ZEval;
import org.gibello.zql.data.ZTuple;

public class SQLparsing {
	
	public static HashMap<String,InputMetaData> myDB = new HashMap<String,InputMetaData>();
	
	public static void main(String args[]){
		int flag=0;
		SQLparsing sp=new SQLparsing();
		 String line;
	    InputMetaData meta=new InputMetaData();
	    
	    
		
		try{
			BufferedReader reader = new BufferedReader(new FileReader("metadata.txt"));
		    
		    while ((line = reader.readLine()) != null)
		    {
		    	if(line.equalsIgnoreCase("<begin_table>")){
		    		flag=1;
		    		meta=new InputMetaData();
		    		meta.tblattributes = new Vector<>();
		    	}
		    	else if(line.equalsIgnoreCase("<end_table>")){
		    		flag=0;
		    		sp.myDB.put(meta.tblname,meta);
		    	}
		    	else if(flag==1){
		    		//System.out.println("tblname:"+line);
		    		meta.tblname=line;
		    		
		    		flag=2;
		    	}
		    	else if(flag==2){
		    		//System.out.println("att: "+line);
		    		meta.tblattributes.add(line);
		    	}
		    	
		      //System.out.println(line);
		    }
		    reader.close();
		}
		 catch (Exception e)
		  {
		    System.err.format("Error!");
		    e.printStackTrace();
		 
		  }
//		System.out.println("***************"+sp.myDB.get("table2").tblname);
//	System.out.println(sp.myDB.containsKey("table1"));
		ClassParser cp=new ClassParser();
		while(true) { 
		System.out.print("mysql> ");
		ZqlParser p = null;
		p = new ZqlParser(System.in);
		cp.mainParsing(p,myDB); 
		//System.out.println("hiiiiiiiiiiiii");
		}
	}
}