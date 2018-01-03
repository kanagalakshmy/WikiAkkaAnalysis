package com.lightbend.akka.sample;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class Wiki {

	public static void main(String[] args) throws IOException {
		File file = new File("C:\\Users\\Divya\\Downloads\\enwiki-latest-all-titles-in-ns0");
		FileReader fileReader = new FileReader(file);
		BufferedReader bufferedReader = new BufferedReader(fileReader);
		String line = "";
		int count = 0;
		while (line != null) {
			line = bufferedReader.readLine();
			// System.out.println(line);
			count++;
		}
		System.out.println("count " + count);
		fileReader.close();
	}

}
