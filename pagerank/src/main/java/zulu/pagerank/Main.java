package zulu.pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import zulu.pagerank.collect.Collect;
import zulu.pagerank.judge.Judge;
import zulu.pagerank.parse.Parse;
import zulu.pagerank.prune.Prune;
import zulu.pagerank.rank.Rank;
import zulu.pagerank.sort.Sort;

public class Main {
	public static void main(String[] args) throws Exception {
		FileSystem hdfs = FileSystem.get(new Configuration());
		long N = 0;
		long error = 0;
		long round = 0;
		int numReducer = Integer.parseInt(args[8]);
		
		System.out.println("# "+numReducer);
		
		//Job 1: Parse
		Parse job1 = new Parse();
        N = job1.parse(args, numReducer);
        
        //Job 2: Pruning
        Prune job2 = new Prune();
        job2.prune(args, N, numReducer);
        
        while (true) {
        	//Job 3: Collect dangling node PR
        	Path danglingPath = new Path(args[3]);
            hdfs.delete(danglingPath, true);
        	
            Collect job3 = new Collect();
            job3.collectDanglingNodePR(args, numReducer);
            
            //Job 4: Do page rank
            Path rankPath = new Path(args[4]);
            hdfs.delete(rankPath, true);
            
    		Rank job4 = new Rank();
            job4.rank(args, N, numReducer);
		
            //Job 5: Judge converge
            Path judgePath = new Path(args[5]);
            hdfs.delete(judgePath, true);
            
        	Judge job5 = new Judge();
        	error = job5.judgeError(args);
        	
        	round++;
        	System.out.println("\n\n============ "+round+"th round end ============");
        	
        	if (error == -1)
        		break;
        	if (args.length == 8 && round == Integer.parseInt(args[7]))
        		break;
        }
        
		//Job 6: Sort
		Sort job6 = new Sort();
		job6.sort(args, numReducer);
        
		System.exit(0);
	}
}