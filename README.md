# Chatbot Concierge #
 - URL: http://chatbot77.s3-website-us-east-1.amazonaws.com/

## About ##
This assignment implements a Dining Concierge chatbot that sends restaurant suggestions 
based on a set of preferences provided by user.  Note: SES is used instead of SNS.

## Extra Creidts ##
For extra credits part, we store user's email address and cuisine type at userSearchRecord table at DynamoDB. If user is already in the database, 
we make extra recommendations based on user's previous inputs. If we did not find user in the database, we simply add user to the database.

## Development Process ##
1. Build and deploy the frontend of the application on AWS S3 bucket.
2. Build the API for the application by using API Gateway.
3. Create a Lambda function (LF0) that performs the chat operation.
4. Build a Dining Concierge chatbot using Amazon Lex.
5. Integrate the Lex chatbot into your chat API.
6. Use the Yelp API to collect 5,000+ random restaurants from Manhattan.
7. Create a DynamoDB table and named “yelp-restaurants” and store the restaurants scrape in DynamoDB.
8. Create an ElasticSearch instance using the AWS ElasticSearch Service.
9. Build a suggestions module, that is decoupled from the Lex chatbot.
10. Set up a CloudWatch event trigger that runs every minute and invokes the Lambda function as a result.
