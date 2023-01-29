package api.lambda;

import api.lambda.model.Product;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.*;

public class ProductLambdaHandler implements RequestStreamHandler {

    private String DYNAMO_TABLE = "Products";

    /**
     * - InputStream: includes all the incoming data.
     * Example, pathParameters, queryStringParameters, request body, request header, etc.
     * = OutputStream: where we send/write the result.
     * Example: We send the data to api gateway.
     * = Context: provides method and properties that provide info about the invocation, function, execution environment, etc.
     * Example: context.getFunctionName();
     */
    @Override
    public void handleRequest(InputStream input, OutputStream output, Context context) throws IOException {
        OutputStreamWriter writer = new OutputStreamWriter(output);
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        JSONParser parser = new JSONParser(); // Will help us parse the request object.
        JSONObject responseObject = new JSONObject(); // We will add to this object for our api response.
        JSONObject responseBody = new JSONObject(); // We will add the item to this object.

        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.defaultClient();
        DynamoDB dynamoDB = new DynamoDB(client);

        int id;
        Item resItem = null;

        try {
            JSONObject reqObject = (JSONObject) parser.parse(reader);
            // pathParameters - Ex: /api/products/1
            if (reqObject.get("pathParameters") != null) {
                JSONObject pps = (JSONObject) reqObject.get("pathParameters");
                if (pps.get("id") != null) {
                    id = Integer.parseInt((String) pps.get("id"));
                    resItem = dynamoDB.getTable(DYNAMO_TABLE).getItem("id", id);
                }
            }

            // queryStringParameters - Ex: /api/products?id=1
            else if (reqObject.get("queryStringParameters") != null) {
                JSONObject qsp = (JSONObject) reqObject.get("queryStringParameters");
                if (qsp.get("id") != null) {
                    id = Integer.parseInt((String) qsp.get("id"));
                    resItem = dynamoDB.getTable(DYNAMO_TABLE).getItem("id", id);
                }
            }

            if (resItem != null) {
                Product product = new Product(resItem.toJSON());
                responseBody.put("product", product);
                responseObject.put("statusCode", 200);
            } else {
                responseBody.put("message", "Not Items Found");
                responseObject.put("statusCode", 404);
            }

            responseObject.put("body", responseBody.toString());
        } catch (Exception e) {
            context.getLogger().log("ERROR: " + e.getMessage());
        }

        writer.write(responseObject.toString());
        reader.close();
        writer.close();
    }

    public void handlePutRequest(InputStream input, OutputStream output, Context context) throws IOException {
        OutputStreamWriter writer = new OutputStreamWriter(output);
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        JSONParser parser = new JSONParser(); // Will help us parse the request object.
        JSONObject responseObject = new JSONObject(); // We will add to this object for our api response.
        JSONObject responseBody = new JSONObject(); // We will add the item to this object.

        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.defaultClient();
        DynamoDB dynamoDB = new DynamoDB(client);

        try {
            JSONObject reqObject = (JSONObject) parser.parse(reader);

            if (reqObject.get("body") != null) {
                Product product = new Product((String) reqObject.get("body"));

                dynamoDB.getTable(DYNAMO_TABLE)
                        .putItem(new PutItemSpec().withItem(new Item()
                                .withNumber("id", product.getId())
                                .withString("name", product.getName())
                                .withNumber("price", product.getPrice())));
                responseBody.put("message", "New Item created/updated");
                responseObject.put("statusCode", 200);
                responseObject.put("body", responseBody.toString());
            }
        } catch (Exception e) {
            responseObject.put("statusCode", 400);
            responseObject.put("error", e);
        }

        writer.write(responseObject.toString());
        reader.close();
        writer.close();
    }

    public void handleDeleteRequest(InputStream input, OutputStream output, Context context) throws IOException {
        OutputStreamWriter writer = new OutputStreamWriter(output);
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        JSONParser parser = new JSONParser(); // Will help us parse the request object.
        JSONObject responseObject = new JSONObject(); // We will add to this object for our api response.
        JSONObject responseBody = new JSONObject(); // We will add the item to this object.

        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.defaultClient();
        DynamoDB dynamoDB = new DynamoDB(client);

        try {
            JSONObject reqObject = (JSONObject) parser.parse(reader);
            if (reqObject.get("pathParameters") != null) {
                JSONObject pps = (JSONObject) reqObject.get("pathParameters");

                if (pps.get("id") != null) {
                    int id = Integer.parseInt((String) pps.get("id"));
                    dynamoDB.getTable(DYNAMO_TABLE).deleteItem("id", id);
                }
            }

            responseBody.put("message", "Item Deleted");
            responseObject.put("statusCode", 200);
            responseObject.put("body", responseBody.toString());
        } catch (Exception e) {
            responseObject.put("statusCode", 400);
            responseObject.put("error", e);
        }

        writer.write(responseObject.toString());
        reader.close();
        writer.close();
    }
    }
