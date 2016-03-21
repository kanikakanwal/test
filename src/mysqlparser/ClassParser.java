package mysqlparser;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;

import org.gibello.zql.ZDelete;
import org.gibello.zql.ZExpression;
import org.gibello.zql.ZInsert;
import org.gibello.zql.ZQuery;
import org.gibello.zql.ZSelectItem;
import org.gibello.zql.ZStatement;
import org.gibello.zql.ZqlParser;

public class ClassParser {

	HashMap<String, InputMetaData> myDB;
	SelectionPrcess sp=new SelectionPrcess();
	boolean distinct=false;
	ZStatement st;
	public void mainParsing(ZqlParser p, HashMap<String, InputMetaData> myDB) {
		try {
			this.myDB = myDB;
		    
		      
		     st = p.readStatement() ;
		     
		     if (st.toString().toLowerCase().indexOf("distinct") >= 0){
		    	 distinct=true;
		    	 //System.out.println("yesssssssssssssssssssss");
		     }
		     
		        //System.out.println(st.toString()); // Display the statement
		     

		        if(st instanceof ZQuery) { // An SQL query: query the DB
		          queryDB((ZQuery)st);
		          //System.out.println("entring: "+st);
		        } else if(st instanceof ZDelete) { // An SQL insert
		          deleteDB((ZDelete)st);
		        }
		        else if(st instanceof ZInsert) { // An SQL insert
			          insertDB((ZInsert)st);
			        }
		      

		    }
		 catch(Exception e) {
		      //e.printStackTrace();
		      String orig=String.valueOf(p.originalString());
		      
		      if(orig.toLowerCase().startsWith("drop")){
		    	  //System.out.println("%%%%%%%%%%%");
		    	  droptbl(orig);
		      }
		      
		      if(orig.toLowerCase().indexOf("distinct")>=0){
		    	  //System.out.println("%%%%%%%%%%%");
		    	  distinct=true;
		    	  //droptbl(orig);
		      }
		      if(orig.toLowerCase().startsWith("truncate")){
		    	  //System.out.println("%%%%%%%%%%%");
		    	  //distinct=true;
		    	  //droptbl(orig);
		    	  truncatetbl(orig);
		      }
		      if(orig.toLowerCase().startsWith("create")){
		    	  //System.out.println("%%%%%%%%%%%");
		    	  //distinct=true;
		    	  //droptbl(orig);
		    	  //truncatetbl(orig);
		    	  //
		    	  Createtbl(orig);
		    	  //System.out.println("%%");
		      }
		    }
		
		//System.out.println("##########");
		
	}
	public int queryDB(ZQuery q) throws Exception{

		
	    Vector<ZSelectItem> sel = q.getSelect(); // SELECT part of the query
	    
	    Vector from = q.getFrom();  // FROM part of the query
	    
	    ZExpression where = (ZExpression)q.getWhere();  // WHERE part of the query
	//System.out.println("sele: "+sel.get(1).getAggregate());
	//System.out.println("from: "+from);
	    //System.out.println("where: "+where);
	String tblname=from.get(0).toString()+".csv";
	
	String tblinput;
	int flag=0;
	for(int i=0;i<from.size();i++){
		tblinput=from.get(i).toString();
		//System.out.println(this.myDB.containsKey("table1"));
		if(!this.myDB.containsKey(tblinput)){
			//System.out.println(this.myDB);
			System.out.println("Table not found");
			flag=1;
		}
	}
		if(flag==1)
			return 0;
		Vector< Vector<Integer>> extab1=new Vector< Vector<Integer>>();
		
		BufferedReader b1 = new BufferedReader(new FileReader(tblname));
		String ip1;
		 while((ip1 = b1.readLine())!=null) 
		 {
			 String token1[] = ip1.split(","); 
			 for(int q1=0;q1<token1.length;q1++){
				// if(token1[q1].contains("\"")){
					 token1[q1]=token1[q1].replace("\"", "");
				// }
			 }
			Vector<Integer> tuple1 = new Vector<Integer>(token1.length); 
			for (int i = 0; i <token1.length; i++)
			 {
				tuple1.add(Integer.parseInt(token1[i]));
			}
			extab1.add(tuple1); 
		}
		 
		 //System.out.println(extab1);
		 //System.out.println("hiiiiiiiiiii");
		 int k=1;
		 Vector< Vector<Integer>> extab3 = extab1;
		while(k<from.size()){
			Vector< Vector<Integer>> extab2=new Vector< Vector<Integer>>();
			tblname=from.get(k).toString()+".csv";
			BufferedReader b2 = new BufferedReader(new FileReader(tblname));
			String ip2;
			 while((ip2 = b2.readLine())!=null) 
			 {
				 String token2[] = ip2.split(","); 
				 for(int q3=0;q3<token2.length;q3++){
						// if(token1[q1].contains("\"")){
							 token2[q3]=token2[q3].replace("\"", "");
						// }
					 }
				Vector<Integer> tuple2 = new Vector<Integer>(token2.length); 
				for (int i = 0; i <token2.length; i++)
				 {
					tuple2.add(Integer.parseInt(token2[i]));
				}
				extab2.add(tuple2); 
			}
			// System.out.println("h##################");

			 //System.out.println("extab"+k+1+extab2);
			 
			 //************ JOIN *******************//
			 extab3=new Vector< Vector<Integer>>();
			 
			 for(int i=0; i<extab1.size();i++){
				 Vector<Integer> t1=extab1.get(i);
				 for(int j=0;j<extab2.size();j++){
					 Vector<Integer> t2=extab2.get(j);
					 Vector<Integer> t3=new Vector<Integer>();
					 
					 t3.addAll(t1);
					 //System.out.println("T1:"+t1);
					 
					 t3.addAll(t2);
					 //System.out.println("T2:"+t2);
					 extab3.add(t3);
					 //System.out.println(t3);
				 }
			 }
			 
			 //System.out.println(extab3);
			 k++;
		}
		//System.out.println("yess"+where);
		
		//System.out.println("############ extab 3#################");
		//System.out.println(extab3);
		
		Vector< Vector<Integer>> resultab=extab3;
		if(where!=null)
			resultab=parsingwhere(extab3,from,where);
			
		resultab=sp.selectQuery(myDB,sel,from,resultab);
		//System.out.println("***********result************");
		
		
		DisplayTuple(resultab);// go for select operation
		
	  return 0;
	}
	 void DisplayTuple(Vector< Vector<Integer>> disptab) {
		//System.out.println("heyyyyy");
		//System.out.println(disptab.toString());
		 
		 if(disptab!=null){
		 if(distinct){
		 
			 for(int i=0;i<disptab.size();i++){
				 Vector<Integer> t=disptab.get(i);
				 for(int j=i+1;j<disptab.size();j++){
					 Vector<Integer> t2=disptab.get(j);
					 if(t.equals(t2)){
					 disptab.remove(j);
					 j--;
					 }
				 }
			 }
//			 System.out.println(disptab);
			 for(int i=0;i<disptab.size();i++){
					Vector<Integer> t1=disptab.get(i);
					for(int j=0;j<t1.size();j++){
						System.out.print(disptab.get(i).get(j)+"   ");
					}
					
					System.out.println();
				}
distinct=false;
		
		 }
		 else{
		for(int i=0;i<disptab.size();i++){
			Vector<Integer> t1=disptab.get(i);
			for(int j=0;j<t1.size();j++){
				System.out.print(disptab.get(i).get(j)+"   ");
			}
			
			System.out.println();
		}
		 }
		 }
	  }
	 Vector< Vector<Integer>> parsingwhere(Vector< Vector<Integer>>tab,Vector from,ZExpression where){
//	System.out.println(where.getOperator());
//	System.out.println(where.getOperands());
	 Vector< Vector<Integer>>result=null;
	int flag=0;
	if(where.getOperator().equalsIgnoreCase("and")||where.getOperator().equalsIgnoreCase("or")){
		ZExpression where1=(ZExpression) where.getOperands().get(0);
		ZExpression where2=(ZExpression) where.getOperands().get(1);
		flag=1;
		//System.out.println(where1.getOperands().get(0));
		String op_1=where1.getOperator();
		String operand1_1=where1.getOperands().get(0).toString();
		String operand2_1=where1.getOperands().get(1).toString();
		String op_2=where2.getOperator();
		String operand1_2=where2.getOperands().get(0).toString();
		String operand2_2=where2.getOperands().get(1).toString();
		Vector< Vector<Integer>>result1=whereOperations(tab,from,op_1,operand1_1,operand2_1);
		Vector< Vector<Integer>>result2=whereOperations(tab,from,op_2,operand1_2,operand2_2);
		//result of common rows
		
		result=new Vector< Vector<Integer>>();
		if(where.getOperator().equalsIgnoreCase("and")){
			
			//And
			
			
			
			
			for(int i=0;i<result1.size();i++){
				Vector<Integer> tuple1=result1.get(i);
				for(int j=0;j<result2.size();j++){
					Vector<Integer> tuple2=result2.get(j);
					if(tuple2.equals(tuple1)){
						result.add(tuple1);
					}
					
				}
			}
			
			
		}
		else if(where.getOperator().equalsIgnoreCase("or")){
			//Or
			
			
//			System.out.println("############result 1####################");
//			System.out.println(result1);
//			System.out.println("############result 2####################");
//			System.out.println(result2);
			
			for(int i=0;i<result1.size();i++){
				Vector<Integer> tuple1=result1.get(i);
				for(int j=0;j<result2.size();j++){
					Vector<Integer> tuple2=result2.get(j);
					if(tuple2.equals(tuple1)&&!result.containsAll(tuple2)&&!result.containsAll(tuple1)){
						result.add(tuple1);
					}
					else if(!result.containsAll(tuple2)&&!result.containsAll(tuple1))
					{
						result.add(tuple1);
						result.add(tuple2);
					}
					
				}
			}
			
		}
		

	}
if(flag==0){
	String op=where.getOperator();
	String operand1=where.getOperands().get(0).toString();
	String operand2=where.getOperands().get(1).toString();
	//System.out.println("op:"+op+"op1: "+operand1+"op2: "+operand2);
	int v=verifyTabAtt(from,operand1,operand2);
	if(v==1)
		return null;
	
	//System.out.println("Done!");
	
	// go for operations
	
	result=whereOperations(tab,from,op,operand1,operand2);
	
	
	
	//go to select
	
	
	}

//	System.out.println("############################################################");
//	System.out.println(result);

return result;
}

	 int verifyTabAtt(Vector from,String op1,String op2){
		 boolean isInt1=false,isStr1=false,isInt2=false,isStr2=false;
//		System.out.println("$$$$$$");
//		 InputMetaData meta=myDB.get(whrtab);
//		 System.out.println(meta);
//		 Vector att=meta.tblattributes;
//		System.out.println("********"+att);
		int flag1=0,flag2=0;
		
		try{
			int in = Integer.parseInt(op2);
			isInt2 = true;
		}
		catch(NumberFormatException ne){
			isStr2 = true;
		}
		try{
			int in = Integer.parseInt(op1);
			isInt1 = true;
		}
		catch(NumberFormatException ne){
			isStr1 = true;
		}
		
		for(int i=0;i<from.size();i++){
//			System.out.println("where: search:"+from.get(i));
//			System.out.println(myDB);
			//System.out.println("%%%%%%"+myDB.get(from.get(i).toString()));
			Vector att=myDB.get(from.get(i).toString()).tblattributes;
//			System.out.println("meta"+att);
			
			
			if(att.contains(op1)&&isStr1){
				flag1++;
				
			}
			else if(isInt1){
				flag1++;
			}
			if(att.contains(op2) && isStr2){
				flag2++;
				
			}
			else if(isInt2){
				flag2++;
			}
		}
		
		if(flag1==0||flag2==0){
			System.out.println("flag:"+"Operands not found");
			return 1;
		}
		
		return 0;
	}
	 Vector< Vector<Integer>> whereOperations(Vector< Vector<Integer>>tab,Vector from,String op,String operand1,String operand2){
	
	
	//find index
	int i1=0,i2=0,I1=0,I2=0;
	boolean isInt1=false,isStr1=false,isInt2=false,isStr2=false;
	
	try{
		int in = Integer.parseInt(operand2);
		isInt2 = true;
	}
	catch(NumberFormatException ne){
		isStr2 = true;
	}
	
	try{
		int in = Integer.parseInt(operand1);
		isInt1 = true;
	}
	catch(NumberFormatException ne){
		isStr1 = true;
	}
	
	for(int i=0;i<from.size();i++){
		if(isStr1)		
		{
		if(myDB.get(from.get(i).toString()).tblattributes.contains(operand1)){
			//System.out.println("yes");
			i1=i1+myDB.get(from.get(i).toString()).tblattributes.indexOf(operand1);
			//System.out.println("found:"+operand1+"at pos:"+i1);
			I1=i1;
		
		}
		else
		{
			i1=i1+myDB.get(from.get(i).toString()).tblattributes.size();
			//System.out.println("finding:"+operand1+"still at pos:"+i1);
		}
		}
		if(isStr2)
		{
		if(myDB.get(from.get(i).toString()).tblattributes.contains(operand2)){
			//System.out.println("yes");
			i2=i2+myDB.get(from.get(i).toString()).tblattributes.indexOf(operand2);
			//System.out.println("found:"+operand2+"at pos:"+i1);
			I2=i2;
		
		}
		else
		{
			i2=i2+myDB.get(from.get(i).toString()).tblattributes.size();
//			System.out.println("finding:"+operand2+"still at pos:"+i1);
		}
		}
		
	}
	
	
	if(isInt2)
		I2=-1;
//	System.out.println("position: op1:"+operand1+" "+I1+" op2:"+I2);
	
	Vector< Vector<Integer>>result = null;
	
	if(op.equals("="))
		result=equalsop(tab,from,op,operand1,operand2,I1,I2);
	
	if(op.equals("<"))
		result=ltop(tab,from,op,operand1,operand2,I1,I2);
	
	if(op.equals(">"))
		result=gtop(tab,from,op,operand1,operand2,I1,I2);
	
	if(op.equals("<="))
		result=lteop(tab,from,op,operand1,operand2,I1,I2);
	
	if(op.equals(">="))
		result=gteop(tab,from,op,operand1,operand2,I1,I2);
	
	return result;
	
	
}


	 Vector< Vector<Integer>> equalsop(Vector< Vector<Integer>>tab,Vector from,String op,String operand1,String operand2, int I1,int I2){
//	System.out.println("hiiii");
	Vector< Vector<Integer>> result=new Vector< Vector<Integer>>();
	for(int i=0;i<tab.size();i++){
		Vector<Integer> tuple = tab.get(i);
		if(I2>0){
//			System.out.println("foundxxxx..");
//			System.out.println("I1:"+tuple.get(I1));
//			System.out.println("I2:"+tuple.get(I2));
//			
			
			if(tuple.get(I1)==tuple.get(I2)){
				//System.out.println("hehehheh");
				result.add(tuple);
//				System.out.println(tuple);
			}
		}
		else
		{
//			System.out.println("op2.."+operand2);
			//matching with A=2;
			
			if(tuple.get(I1)==Integer.parseInt(operand2)){
//				System.out.println("found");
				result.add(tuple);
			}
		}
	}
	
	//System.out.println(result);
	return result;
	
}
	 
	 Vector< Vector<Integer>> ltop(Vector< Vector<Integer>>tab,Vector from,String op,String operand1,String operand2, int I1,int I2){
			//System.out.println("lt hiiii");
			Vector< Vector<Integer>> result=new Vector< Vector<Integer>>();
			for(int i=0;i<tab.size();i++){
				Vector<Integer> tuple = tab.get(i);
				if(I2>0){
					//System.out.println("foundxxxx..");
//					System.out.println("I1:"+tuple.get(I1));
//					System.out.println("I2:"+tuple.get(I2));
					
					
					if(tuple.get(I1)<tuple.get(I2)){
						//System.out.println("hehehheh");
						result.add(tuple);
//						System.out.println(tuple);
					}
				}
				else
				{
//					System.out.println("op2.."+operand2);
					//matching with A=2;
					
					if(tuple.get(I1)<Integer.parseInt(operand2)){
						//System.out.println("found");
						result.add(tuple);
					}
				}
			}
			
			//System.out.println(result);
			return result;
			
		}
	 Vector< Vector<Integer>> gtop(Vector< Vector<Integer>>tab,Vector from,String op,String operand1,String operand2, int I1,int I2){
			//System.out.println("lt hiiii");
			Vector< Vector<Integer>> result=new Vector< Vector<Integer>>();
			for(int i=0;i<tab.size();i++){
				Vector<Integer> tuple = tab.get(i);
				if(I2>0){
					//System.out.println("foundxxxx..");
//					System.out.println("I1:"+tuple.get(I1));
//					System.out.println("I2:"+tuple.get(I2));
					
					
					if(tuple.get(I1)>tuple.get(I2)){
						//System.out.println("hehehheh");
						result.add(tuple);
//						System.out.println(tuple);
					}
				}
				else
				{
					//System.out.println("op2.."+operand2);
					//matching with A=2;
					
					if(tuple.get(I1)>Integer.parseInt(operand2)){
						//System.out.println("found");
						result.add(tuple);
					}
				}
			}
			
			//System.out.println(result);
			return result;
			
		}

	 
	 
	 Vector< Vector<Integer>> lteop(Vector< Vector<Integer>>tab,Vector from,String op,String operand1,String operand2, int I1,int I2){
			//System.out.println("lt hiiii");
			Vector< Vector<Integer>> result=new Vector< Vector<Integer>>();
			for(int i=0;i<tab.size();i++){
				Vector<Integer> tuple = tab.get(i);
				if(I2>0){
					//System.out.println("foundxxxx..");
//					System.out.println("I1:"+tuple.get(I1));
//					System.out.println("I2:"+tuple.get(I2));
					
					
					if(tuple.get(I1)<=tuple.get(I2)){
						//System.out.println("hehehheh");
						result.add(tuple);
//						System.out.println(tuple);
					}
				}
				else
				{
//					System.out.println("op2.."+operand2);
					//matching with A=2;
					
					if(tuple.get(I1)<=Integer.parseInt(operand2)){
						//System.out.println("found");
						result.add(tuple);
					}
				}
			}
			
			//System.out.println(result);
			return result;
			
		}
	 Vector< Vector<Integer>> gteop(Vector< Vector<Integer>>tab,Vector from,String op,String operand1,String operand2, int I1,int I2){
			//System.out.println("lt hiiii");
			Vector< Vector<Integer>> result=new Vector< Vector<Integer>>();
			for(int i=0;i<tab.size();i++){
				Vector<Integer> tuple = tab.get(i);
				if(I2>0){
					//System.out.println("foundxxxx..");
//					System.out.println("I1:"+tuple.get(I1));
//					System.out.println("I2:"+tuple.get(I2));
					
					
					if(tuple.get(I1)>=tuple.get(I2)){
						//System.out.println("hehehheh");
						result.add(tuple);
//						System.out.println(tuple);
					}
				}
				else
				{
//					System.out.println("op2.."+operand2);
					//matching with A=2;
					
					if(tuple.get(I1)>=Integer.parseInt(operand2)){
						//System.out.println("found");
						result.add(tuple);
					}
				}
			}
			
			//System.out.println(result);
			return result;
			
		}



	  public void insertDB(ZInsert ins) throws Exception {
//	    System.out.println("Should implement INSERT here");
//	    System.out.println(ins.getTable());
//	    Vector columns_=ins.getColumns();
//	    System.out.println("col:"+columns_);
//	    System.out.println("val:"+ins.getValues());
//	    System.out.println(ins.toString());
	    
	    Vector col;
	    Vector val=ins.getValues();
	    
	    if(ins.getColumns()==null)
	    	col=myDB.get(ins.getTable()).tblattributes;
	    else
	    	col=ins.getColumns();
	    //System.out.println("colss:"+ins.getTable());
	    StringBuilder sb = new StringBuilder();
	    BufferedReader br = new BufferedReader(new FileReader(ins.getTable()+".csv"));
	    PrintWriter bw = new PrintWriter(new FileWriter(ins.getTable()+".csv",true));
	    bw.flush();
	    if (br.readLine() != null) { 
	    	   	 
	    	 
	    	 
	    	 sb.append("\n");
	    	 //System.out.println(br.readLine());
	    	 
	    	 
	    	}
	    
	    //System.out.println("val:"+val);
	    for(int i=0;i<val.size();i++){
	    	//bw.append((char) Integer.parseInt( (String) val.get(i)));
//	    	System.out.println(val.get(i).toString());
	    	//bw.write(val.get(i).toString());
	    	sb.append(val.get(i).toString());
	    	if(i<col.size()-1){
	    		sb.append(",");
	    	}
	    }
	    
	    
	    bw.write(sb.toString());
	    System.out.println("1 row created.");
	    bw.flush();
	    br.close();
	    bw.close();
	    }


	    public void deleteDB(ZDelete del)throws Exception{
	    	ZExpression where=(ZExpression) del.getWhere();
	    	//System.out.println(where);
//	    	System.out.println(del.getTable());
	    	
	    	
	    	Vector< Vector<Integer>> extab1=new Vector< Vector<Integer>>();
			
			BufferedReader b1 = new BufferedReader(new FileReader(del.getTable()+".csv"));
			
			String ip1;
			 while((ip1 = b1.readLine())!=null) 
			 {
				 String token1[] = ip1.split(","); 
				Vector<Integer> tuple1 = new Vector<Integer>(token1.length); 
				for (int i = 0; i <token1.length; i++)
				 {
					tuple1.add(Integer.parseInt(token1[i]));
				}
				extab1.add(tuple1); 
			}
			 Vector< Vector<Integer>> resultab=extab1;
	    	if(where!=null)
	    		resultab=parsingDelwhere(extab1,del.getTable(),where);
	    	else
	    	{
	    		resultab=extab1;
	    	}
	    	
	    	BufferedWriter b2=new BufferedWriter (new FileWriter(del.getTable()+".csv"));
	    	
//	    	System.out.println("re size>>>>>>>>>>"+resultab.size());
	    	for(int i=0;i<resultab.size();i++){
	    		StringBuilder sb = new StringBuilder();
	    		Vector<Integer> t=new Vector<Integer>();
	    		t=resultab.get(i);
	    		for(int j=0;j<t.size();j++){
	    			Integer n=t.get(j);
	    			//System.out.println(n);
	    			sb.append(n.toString());
	    			if(j<t.size()-1)
	    			sb.append(",");
	    			
	    		}
	    		sb.append("\n");
	    		b2.write(sb.toString());
	    		b2.flush();
	    	}
	    	
	    	b2.close();
				
	    }
	    
	    
	    public Vector< Vector<Integer>> parsingDelwhere(Vector< Vector<Integer>> deltab, String tblname, ZExpression where){
	    	
	    	String op=where.getOperator();
	    	String op1=where.getOperands().get(0).toString();
	    	String op2=where.getOperands().get(1).toString();
	    	
	    	Vector myFrom=new Vector();
	    	myFrom.add(tblname);
	    	//System.out.println("op:"+op+"op1: "+operand1+"op2: "+operand2);
	    	int v=verifyTabAtt(myFrom,op1,op2);
	    	if(v==1)
	    		return null;
//	    	System.out.println("done");
	    	
	    	Vector< Vector<Integer>> wheretab=null;
	    	Vector< Vector<Integer>> resultab=new Vector< Vector<Integer>>() ;
	    	
	    	wheretab=whereOperations(deltab,myFrom,op,op1,op2);
//	    	System.out.println(wheretab);
	    	int flag=0;
	    	for(int i=0;i<deltab.size();i++){
	    		flag=0;
	    		for(int j=0;j<wheretab.size();j++){
	    			if(deltab.get(i).equals(wheretab.get(j))){
	    				flag=1;
	    			}
	    			if(flag==0){
	    				resultab.add(deltab.get(i));
	    			}
	    		}
	    	}
	    	
	    	System.out.println(resultab);
	    	
	    	return resultab;
	    }
	    
	    public void truncatetbl(String orig){
	    	String[] splited = orig.toString().split("\\s+");
	    	String tblname=splited[2];
	    	tblname=tblname.replace(";","");
	    	System.out.println(tblname);
	    	if(!myDB.containsKey(tblname)){
	    		System.out.println("Invalid table");
	    	}
	    	else{
	    		try {
					BufferedWriter b2=new BufferedWriter (new FileWriter(tblname+".csv"));
					b2.close();
					
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	    		System.out.println("Success!");

	    	}
	    }
	    
	    public void droptbl(String orig){
	    	String[] splited = orig.toString().split("\\s+");
	    	String tblname=splited[2];
//	    	System.out.println(tblname);
	    	tblname=tblname.replace(";","");
//	    	System.out.println(tblname);
	    	if(!myDB.containsKey(tblname)){
	    		System.out.println("Invalid table");
	    	}
	    	else
	    	{
				try {
					BufferedReader b1 = new BufferedReader(new FileReader(tblname+".csv"));
					if(b1.readLine()!=null){
						System.out.println("Table is not empty");
					}
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
	    		
	    		myDB.remove(tblname);
	    		
		    	try {
					BufferedWriter b2=new BufferedWriter (new FileWriter("metadata.txt"));
					//Vector tab = myDB.values();
					StringBuilder sb = new StringBuilder();
					for(InputMetaData m: myDB.values()){
						
						sb.append("<begin_table>\n");
						sb.append(m.tblname);
						sb.append("\n");
						for(int i=0;i<m.tblattributes.size();i++){
							sb.append(m.tblattributes.get(i).toString());
							
								sb.append("\n");
						}
						sb.append("<end_table>\n");
						
					}
					b2.write(sb.toString());
					b2.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		    	System.out.println("table drop");

	    	}
	    	
	    		    }
	    
	    //create table tb3 (G int, H int, I int);
	    public void Createtbl(String orig){
	    	
	    	String temp = orig.toString().replace('(', ' ');
	    	temp = temp.replace(')', ' ');
	    	temp = temp.replace(',', ' ');
	    	
	    	String[] splited = temp.split("\\s+");
	    	String tbName = splited[2];
	    	//System.out.println(tbName);
	    	if(myDB.containsKey(tbName)){
	    		System.out.println("Table exists!");
	    		
	    	}
	    	else
	    	{
	    		
	    		Vector<String> list = new Vector<String>();
		    	for(int m=3; m < splited.length-2; m++){
		    		if(!splited[m].equals("int")){
		    			list.add(splited[m]);
		    		}
		    	}
		    	
		    	System.out.println(list);
		    	
		    	
	    		
	    		InputMetaData arg1 = new InputMetaData();
	    		arg1.tblname=tbName;
	    		Vector<String> l=new Vector<String>();
	    		for(String in : list){
	    			l.add(in);
	    		}
	    		arg1.tblattributes=l;
				myDB.put(tbName, arg1 );
	    		
		    	try {
					BufferedWriter b2=new BufferedWriter (new FileWriter("metadata.txt"));
					//Vector tab = myDB.values();
					StringBuilder sb = new StringBuilder();
					for(InputMetaData m: myDB.values()){
						
						sb.append("<begin_table>\n");
						sb.append(m.tblname);
						sb.append("\n");
						for(int i=0;i<m.tblattributes.size();i++){
							sb.append(m.tblattributes.get(i).toString());
							
								sb.append("\n");
						}
						sb.append("<end_table>\n");
						
					}
					
					
					
					b2.write(sb.toString());
					b2.close();
					BufferedWriter b3=new BufferedWriter (new FileWriter(tbName+".csv"));
					b3.close();

				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	System.out.println("1 table created.");
	    	}
	    }
	    
	  }
	

