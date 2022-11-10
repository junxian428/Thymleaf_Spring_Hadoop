package com.example.demo;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.springframework.stereotype.Controller;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

@Controller
public class UploadController {

    //private final String UPLOAD_DIR = "./uploads/";
    private final String UPLOAD_DIR = "./uploads/";

    @GetMapping("/")
    public String homepage() {
        return "index";
    }



    @PostMapping("/upload")
    public String uploadFile(@RequestParam("file") MultipartFile file, RedirectAttributes attributes) throws IOException, URISyntaxException {

        // check if file is empty
        if (file.isEmpty()) {
            attributes.addFlashAttribute("message", "Please select a file to upload.");
            return "redirect:/";
        }
        String filename = file.getOriginalFilename();
        final String imagePath = "C:\\Users\\junxian428\\Downloads\\demo (5)\\demo\\src\\web\\images\\"; //path
        FileOutputStream output = new FileOutputStream(imagePath+filename);
        System.out.println(filename);
        output.write(file.getBytes());
        try{
            URI uri = new URI("hdfs://:9000/");
            Configuration configuration = new Configuration();
            FileSystem fs = FileSystem.get(uri,configuration);
            fs.moveFromLocalFile(new org.apache.hadoop.fs.Path(imagePath+filename), new org.apache.hadoop.fs.Path("hdfs://:9000/"+filename));
       
    
            // return success response
            attributes.addFlashAttribute("message", "You successfully uploaded " + filename + '!');
    
            return "redirect:/";

        }catch(Exception e){
            System.out.println(e.getStackTrace());
            return "error";
        }
    
    }


    
}
