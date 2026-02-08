package org.howard.edu.lsp.assignment2;

import java.io.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;

public class ETLPipeline {
    public static void main(String[] args) {
        System.out.println("Current directory: " + System.getProperty("user.dir"));


        String product = "data/products.csv";
        String transformed = "data/transformed_products.csv";

        List<String> newTransformedList = new ArrayList<>();

        File inputFile = new File(product);
        if (!inputFile.exists()) {
            System.out.println("Error: Input file not found at " + product);
            return;
        }

        try{
            BufferedReader reader = new BufferedReader(new FileReader(product));
            String line = "";
            String header = reader.readLine();
            while((line= reader.readLine())!=null){

                if (line.trim().isEmpty()) {
                    continue;
                }
                String[] list = line.split(",");
                for (int i=0;i< list.length;i++){
                    list[i] = list[i].trim();
                }
                if (list.length != 4) {
                    continue;
                }
                try {
                    int productId = Integer.parseInt(list[0]);
                    double price = Double.parseDouble(list[2]);
                    String name = list[1];
                    String category = list[3];

                    String originalCategory = category;
                    name = name.toUpperCase();

                    if (category.equals("Electronics")) {
                        price = price * 0.9;
                    }
                    BigDecimal rounding = new BigDecimal(price);
                    rounding = rounding.setScale(2, RoundingMode.HALF_UP);
                    price = rounding.doubleValue();

                    if (price > 500.00 && originalCategory.equals("Electronics")) {
                        category = "Premium Electronics";
                    }

                    String priceRange;
                    if(price<=10.00){
                        priceRange = "Low";
                    }else if(price<=100.00){
                        priceRange ="Medium";
                    } else if(price<= 500.00){
                        priceRange = "High";
                    } else {
                        priceRange = "Premium";
                    }

                    String formattedPrice = String.format("%.2f", price);
                    String transformedRow = productId + "," + name + "," + formattedPrice + "," + category + "," + priceRange;
                    newTransformedList.add(transformedRow);

                } catch (NumberFormatException e) {
                    System.out.println("There was a problem with the file: "+ e);
                }

            }
            reader.close();

            BufferedWriter newOutput = new BufferedWriter(new FileWriter(transformed));
            newOutput.write("ProductID,Name,Price,Category,PriceRange");
            newOutput.newLine();

            for (String row : newTransformedList) {
                newOutput.write(row);
                newOutput.newLine();
            }
            newOutput.close();

        }catch(IOException e){
            System.out.println("There was an error reading file: " + e.getMessage());
        }
    }
}
