package com.huike;

import org.apache.hadoop.util.ProgramDriver;

import com.huike.action01.Average;
import com.huike.action02.TopN;
import com.huike.action03.MaxNum;
import com.huike.action04.SecondarySort;
import com.huike.action05.InverseIndex;
import com.huike.action06.MailMultipleOutput;
import com.huike.action07.WeiboMultipleOutput;
import com.huike.action08.Score;
import com.huike.action09.SearchIndex;
import com.huike.action10.Sort;
import com.huike.hdfs.oper.CopyManyFilesToHDFS;
import com.huike.hdfs.oper.MergeSmallFile;
import com.huike.hdfs.oper.SimpleOperation;
import com.huike.join.MapJoin;
import com.huike.join.ReduceJoin;
import com.huike.join.ReduceJoinSecond;
import com.huike.join.SemiJoin;
import com.huike.test00.WordCount;
import com.huike.test01.Patent;
import com.huike.test02.Salary;
import com.huike.test03.Temperature;
import com.huike.test04.Anagram;

/**
 * A description of an example program based on its class and a human-readable
 * description.
 */
public class MyDriver {

	public static void main(String argv[]) {
		int exitCode = -1;
		ProgramDriver programDriver = new ProgramDriver();
		try {
			programDriver.addClass("WordCount", WordCount.class,
					"A map/reduce program that counts the words in the input files.");
			programDriver.addClass("Patent", Patent.class, "Patent");
			programDriver.addClass("Salary", Salary.class, "Salary");
			programDriver.addClass("Temperature", Temperature.class, "Temperature");
			programDriver.addClass("Anagram", Anagram.class, "Anagram");

			programDriver.addClass("Average", Average.class, "Average");
			programDriver.addClass("TopN", TopN.class, "TopN");
			programDriver.addClass("MaxNum", MaxNum.class, "MaxNum");
			programDriver.addClass("SecondarySort", SecondarySort.class, "SecondarySort");
			programDriver.addClass("InverseIndex", InverseIndex.class, "InverseIndex");
			programDriver.addClass("MailMultipleOutput", MailMultipleOutput.class, "MailMultipleOutput");
			programDriver.addClass("WeiboMultipleOutput", WeiboMultipleOutput.class, "WeiboMultipleOutput");
			programDriver.addClass("Score", Score.class, "Score");
			programDriver.addClass("SearchIndex", SearchIndex.class, "SearchIndex");
			programDriver.addClass("Sort", Sort.class, "Sort");

			programDriver.addClass("MapJoin", MapJoin.class, "MapJoin");
			programDriver.addClass("ReduceJoin", ReduceJoin.class, "ReduceJoin");
			programDriver.addClass("ReduceJoinSecond", ReduceJoinSecond.class, "ReduceJoinSecond");
			programDriver.addClass("SemiJoin", SemiJoin.class, "SemiJoin");

			programDriver.addClass("SimpleOperation", SimpleOperation.class, "SimpleOperation");
			programDriver.addClass("CopyManyFilesToHDFS", CopyManyFilesToHDFS.class, "CopyManyFilesToHDFS");
			programDriver.addClass("MergeSmallFile", MergeSmallFile.class, "MergeSmallFile");
			
			exitCode = programDriver.run(argv);
		} catch (Throwable e) {
			e.printStackTrace();
		}

		System.exit(exitCode);
	}
}