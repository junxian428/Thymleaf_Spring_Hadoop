package com.example.demo;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class DisplayController {



    @ModelAttribute("messages")
    public ArrayList messages() throws URISyntaxException, IOException {
        ArrayList<String> FileCollection = new ArrayList<String>();
        URI uri = new URI("hdfs://20.213.9.250:9000/");
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(uri,configuration);
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while(listFiles.hasNext() ){
            LocatedFileStatus fileStatus = listFiles.next();
            FileCollection.add(fileStatus.getPath().toString());
            FileCollection.add(fileStatus.getPermission().toString());
            FileCollection.add(fileStatus.getPermission().toString());
            FileCollection.add(fileStatus.getOwner().toString());
            FileCollection.add(fileStatus.getPath().getName());

        }
        return FileCollection;
    }

    @GetMapping("/message")
    public ArrayList listOfFile() throws URISyntaxException, IOException{
        ArrayList<String> FileCollection = new ArrayList<String>();
        URI uri = new URI("hdfs://20.213.9.250:9000/");
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(uri,configuration);
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while(listFiles.hasNext() ){
            LocatedFileStatus fileStatus = listFiles.next();
            FileCollection.add(fileStatus.getPath().toString());
            FileCollection.add(fileStatus.getPermission().toString());
            FileCollection.add(fileStatus.getPermission().toString());
            FileCollection.add(fileStatus.getOwner().toString());
            FileCollection.add(fileStatus.getPath().getName());

        }
        return FileCollection;
    }
}
