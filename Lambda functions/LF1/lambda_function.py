import json
import boto3
from botocore.exceptions import ClientError

#-------- send sqs message--------
def send_sqs_message(slots):
    print("---------send sqs message----------")
    # Send the SQS message
    sqs_client = boto3.client('sqs')
    sqs_queue_url = "https://sqs.us-east-1.amazonaws.com/442522652070/Q1"
    msg_attributs = {
        "Location": {
            "DataType": "String",
            "StringValue":slots["Location"]
        },
        "Cuisine": {
            "DataType": "String",
            "StringValue":slots["Cuisine"]
        },
        "Date": {
            "DataType": "String",
            "StringValue":slots["Date"]
        },
        "Time": {
            "DataType": "String",
            "StringValue":slots["Time"]
        },
        "NumberOfPeople": {
            "DataType": "String",
            "StringValue":slots["NumberOfPeople"]
        },
         "Email": {
            "DataType": "String",
            "StringValue":slots["Email"]
        }
    }
    msg_body = "Requestion Information"
    try:
        response = sqs_client.send_message(QueueUrl=sqs_queue_url, MessageAttributes=msg_attributs, MessageBody=msg_body) 
    except ClientError as e:
        logging.error(e)
 
    
#---------------intents------------
# greeting intent
def greeting(intent_request):     
    if "sessionAttributes" in intent_request:
        session_attributes = intent_request["sessionAttributes"]
    else:
        session_attributes = {}
    message = {'contentType': 'PlainText', 'content': "Hi there, how can I help you?"}
    return elicit_intent(session_attributes, message)

# dining suggestion intent
def dining(intent_request):
    print("----inside dining function-------")
    slots = intent_request["currentIntent"]["slots"]
    location = slots["Location"]
    cuisine = slots["Cuisine"]
    date = slots["Date"]
    time = slots["Time"]
    number_of_people = slots["NumberOfPeople"]
    email = slots["Email"]
    
    source = intent_request['invocationSource']
    if "sessionAttributes" in intent_request:
        session_attributes = intent_request["sessionAttributes"]
    else:
        session_attributes = {}
    
    if source =='DialogCodeHook':
        print("----inside dialog hook--------")
        # validate location, ask user to input Manhattan if other input location is found
        if(location is not None and location.lower() != 'manhattan'):
            print("----not manhattan-------")
            message =  {"contentType": "PlainText", "content": "Sorry, {} is not a supported location. Please enter a valid location. e.g.Manhattan".format(location)}
            slots["Location"] = None
            return elicit_slot(session_attributes, 
                               intent_request["currentIntent"]["name"], 
                                intent_request["currentIntent"]["slots"],
                                "Location",
                                message)
                                
        # validate cuisine
        cuisines = ["chinese", "japanese", "american", "italian", "korean", "mexican", "indian"]
        if(cuisine is not None and cuisine.lower() not in cuisines):
            message =  {"contentType": "PlainText", "content": "Sorry, {} is not a valid cuisine type. Please enter a valid one.".format(cuisine)}
            slots["Cuisine"] = None
            return elicit_slot(session_attributes, 
                               intent_request["currentIntent"]["name"], 
                               intent_request["currentIntent"]["slots"],
                                "Cuisine",
                                message)
                                
        # validate number of people
        # validate date
        # validate time
        return delegate(session_attributes, intent_request["currentIntent"]["slots"])
        
    # send sqs message
    send_sqs_message(intent_request["currentIntent"]["slots"])
    # return close dialog
    message = {'contentType': 'PlainText', 'content': "You’re all set. Expect my suggestions shortly! Have a good day."}
    return close(session_attributes, message)

#  thank you intent
def thank_you(intent_request):
    if "sessionAttributes" in intent_request:
        session_attributes = intent_request["sessionAttributes"]
    else:
        session_attributes = {}
    session_attributes = intent_request["sessionAttributes"]
    message = {'contentType': 'PlainText', 'content': "You are welcome."}
    return close(session_attributes, message)

#---------different type of dialogs-----------
#  elicit slot: The next action is to elicit a slot value from the user. 
def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message):
    response = {
        "sessionAttributes": session_attributes,
        "dialogAction": {
            "type": "ElicitSlot",
            "intentName": intent_name,
            "slots": slots,
            "slotToElicit": slot_to_elicit,
            "message": message
        }
    }
    return response
    
# elicit intent dialog
def elicit_intent(session_attributes, message):
    response = {
        "sessionAttributes": session_attributes,
        "dialogAction":{
            "type": "ElicitIntent",
            "message": message
        }
    }
    return response

# close dialog
def close(session_attributes, message):
    response = {
        "sessionAttributes": session_attributes,
        "dialogAction":{
            "type": "Close",
            "fulfillmentState": "Fulfilled",
            "message": message
        }
    }
    return response

# delegate dialog 
def delegate(session_attributes, slots):
    response = {
        "sessionAttributes": session_attributes,
        "dialogAction": {
            "type": "Delegate",
            "slots": slots
        }
    } 
    return response


# ----------function to dispatch intents------------
def dispatch(intent_request):
    intent_name = intent_request["currentIntent"]["name"]
    
    if intent_name == "GreetingIntent":
        return greeting(intent_request)
    if intent_name == "ThankYouIntent":
        return thank_you(intent_request)
    if intent_name == "DiningSuggestionsIntent":
        return dining(intent_request)
    print("ERROR No matching: ", intent_name)
  

# -----------main lambda handler------------
def lambda_handler(event, context):
    # @TODO error case handle
    return_value = dispatch(event)
    return return_value
