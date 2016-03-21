package mysqlparser;

import java.util.HashMap;
import java.util.Vector;

import org.gibello.zql.ZSelectItem;

public class SelectionPrcess {
	
	public Vector<Vector<Integer>> selectQuery(HashMap<String, InputMetaData> myDB,Vector<ZSelectItem> sel, Vector from, Vector<Vector<Integer>> result) {
		int ind = 0;
		int flag=0;
		Vector<Integer> temp = new Vector<Integer>();
		Vector<Vector<Integer>> seltab = new Vector<Vector<Integer>>();
		
		if(sel.get(0).toString().equals("*")){
			seltab = result;
		}

			else{
				for(int i=0;i<sel.size();i++){
					if(sel.get(i).getAggregate()!=null){
						flag=1;
						//System.out.println("no nul");
					}
				}
				
				if(flag==1){
					// aggregate functions
					
					int max=0,min=0,sum=0;
					float avg=0;
					
					
					String agg=sel.get(0).getAggregate();
					String col=sel.get(0).getColumn();
					//System.out.println("agg: "+agg+"on "+col);
					int index=myDB.get(from.get(0).toString()).tblattributes.indexOf(sel.get(0).getColumn());
					if(index==-1){
						System.out.println("Invalid column name!");
						return null;
					}
					//System.out.println("index: "+index);
					if(agg.equalsIgnoreCase("max")){
						Vector<Integer> t =result.get(0);
						max=t.get(index);
//						System.out.println("max"+max);
						for(int x=0;x<result.size();x++){
							t=result.get(x);
							
								if(max<t.get(index))
									max=t.get(index);
							
							
						}
						t=new Vector<Integer>();
						t.add(max);
						seltab.add(t);
					}
					else if(agg.equalsIgnoreCase("min")){
						Vector<Integer> t =result.get(0);
						min=t.get(index);
						//System.out.println("max"+max);
						for(int x=0;x<result.size();x++){
							t=result.get(x);
							
								if(min>t.get(index))
									min=t.get(index);
							
							
						}
						t=new Vector<Integer>();
						t.add(min);
						seltab.add(t);
					}
					else if(agg.equalsIgnoreCase("avg")){
						Vector<Integer> t =null;
						
						//System.out.println("max"+max);
						for(int x=0;x<result.size();x++){
							t=result.get(x);
							
								avg=avg+t.get(index);
									
							
							
						}
						System.out.println("sun:"+avg);
						avg=avg/result.size();
						System.out.println("r:"+result.size());
						System.out.println(avg);
						seltab=null;
					}
					
					else if(agg.equalsIgnoreCase("sum")){
						Vector<Integer> t =null;
						
						//System.out.println("max"+max);
						for(int x=0;x<result.size();x++){
							t=result.get(x);
							
								sum=sum+t.get(index);
									
							
							
						}
						
						System.out.println(sum);
						seltab=null;
					}
					else{
						System.out.println("Invalid");
					}
					
				}
				
				else{

					for(int i=0;i<sel.size();i++){
						ind=0;
						for(int j=0;j<from.size();j++){
							//System.out.println("search: "+);
							if(myDB.get(from.get(j).toString()).tblattributes.contains(sel.get(i).toString())){
								ind=ind+myDB.get(from.get(j).toString()).tblattributes.indexOf(sel.get(i).toString());
//								System.out.println("found: "+ind);
								temp.add(ind);
							}
							else
								ind=ind+myDB.get(from.get(j).toString()).tblattributes.size();
						}
						
					}
			if(temp.contains(-1)||temp.size()==0){
				System.out.println("Invalid column name!");
				return null;
			}
					
				//	System.out.println("indexes:"+temp);
				
						for(int i=0;i<result.size();i++){
							Vector<Integer> tuple=result.get(i);
							Vector<Integer> selTuple = new Vector<Integer>();
							for(int j=0;j<temp.size();j++){
								selTuple.add(tuple.get(temp.get(j)));
								}
							
							seltab.add(selTuple);
						}
				}
				
			}
				
				//System.out.println(seltab);
		

		return seltab;
	

	
	}
	
	

}
