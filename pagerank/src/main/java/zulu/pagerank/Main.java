package zulu.pagerank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import zulu.pagerank.build.Build;
import zulu.pagerank.collect.Collect;
import zulu.pagerank.judge.Judge;
import zulu.pagerank.parse.Parse;
import zulu.pagerank.rank.Rank;
import zulu.pagerank.sort.Sort;

public class Main {
	public static void main(String[] args) throws Exception {
		long N = 0;
		long error = 0;
		long round = 0;
		int numReducer = Integer.parseInt(args[8]);
		
		System.out.println("# "+numReducer);
		
		//Job 1: Parse
		Parse job1 = new Parse();
        N = job1.parse(args, numReducer);
        
        //Job 2: Pruning
        Build job2 = new Build();
        job2.buildGraph(args, N, numReducer);
        
        deleteFile(args[1]);
        
        while (true) {
        	//Job 3: Collect dangling node PR
            Collect job3 = new Collect();
            job3.collectDanglingNodePR(args, numReducer);
            
            //Job 4: Do page rank
            deleteFile(args[4]);
            
    		Rank job4 = new Rank();
            job4.rank(args, N, numReducer);
		
            //Job 5: Judge converge
            deleteFile(args[3]);
            
        	Judge job5 = new Judge();
        	error = job5.judgeError(args);
        	
        	deleteFile(args[5]);
        	
        	round++;
        	System.out.println("\n\n============ "+round+"th round end ============");
        	
        	if (error == -1)
        		break;
        	if (args.length == 9 && round == Integer.parseInt(args[7]))
        		break;
        }
        
		//Job 6: Sort
		Sort job6 = new Sort();
		job6.sort(args, numReducer);
        
		System.exit(0);
	}
	
	public static void deleteFile(String path) throws IOException {
		FileSystem hdfs = FileSystem.get(new Configuration());
        hdfs.delete(new Path(path), true);
	}
}