
#include "config.h"

#include <string.h>

#include "mosquitto_broker_internal.h"

void log_sub(char *sub){
    log__printf(NULL, MOSQ_LOG_DEBUG, "\t %s", sub);
}

bool has_tasks_qos(char *sub){
    //log__printf(NULL, MOSQ_LOG_DEBUG, "\t in has_lat_qos");
    char *tasksStr = "tasks";
    char* result = strstr(sub, tasksStr);
    if(result == NULL){
        //log__printf(NULL, MOSQ_LOG_DEBUG, "\t in has_lat_qos if statement");
        return false;
    }
    return true;
}

void printStmtResults(sqlite3_stmt *stmt){
    int rc;
    int i; 
    int columnNum = sqlite3_column_count(stmt);
    for (i = 0; i < columnNum; i++){
            log__printf(NULL, MOSQ_LOG_DEBUG, "%s = %s \n", sqlite3_column_name(stmt, i), sqlite3_column_text(stmt,i));
    }
    while((rc = sqlite3_step(stmt)) == SQLITE_ROW){
        int columnNum = sqlite3_column_count(stmt);
        for (i = 0; i < columnNum; i++){
            log__printf(NULL, MOSQ_LOG_DEBUG, "%s = %s \n", sqlite3_column_name(stmt, i), sqlite3_column_text(stmt,i));
        }
        log__printf(NULL, MOSQ_LOG_DEBUG, "\n");
    }
}

// Need full mosquitto context to extract clientid of the subscriber that send incoming_sub
void store_lat_qos(struct mosquitto *context, char* sub_with_lat_qos){
    //log__printf(NULL, MOSQ_LOG_DEBUG, "\t in store_lat_qos");
    char *latencyStr = "%latency%";
    //log__printf(NULL, MOSQ_LOG_DEBUG, "\t before strstr");
    char* result = strstr(sub_with_lat_qos, latencyStr); // result points at %latency%* in sub_with_lat_qos
    //looks for %latency%
    //log__printf(NULL, MOSQ_LOG_DEBUG, "\t after strstr");
    size_t latStr_len = strlen(result); 
    //allocate the necessary memory for holding just the latency in context
    //log__printf(NULL, MOSQ_LOG_DEBUG, "\t before allcoating mem to temp_lat_qos");
    char* temp_lat_qos = malloc(latStr_len - 7);
    
    strcpy(temp_lat_qos, result + 9); // ignores the %latency% substring, keeps the numbers afterward
    //log__printf(NULL, MOSQ_LOG_DEBUG, "\t after strcpy");
    context->mqtt_cc.incoming_lat_qos = atoi(temp_lat_qos);
    //log__printf(NULL, MOSQ_LOG_DEBUG, "\t Latency QoS: %d", context->mqtt_cc.incoming_lat_qos);
    // remove the latency qos from the subscription
    while(*result){
            *result = *(result + latStr_len);
            result++;
    }
    // save the sub, which no longer has the latency qos attached
    context->mqtt_cc.incoming_topic = sub_with_lat_qos; 
    context->mqtt_cc.incoming_sub_clientid = context->id;
    //log__printf(NULL, MOSQ_LOG_DEBUG, "\t For Topic: %s", context->mqtt_cc.incoming_topic);
    //log__printf(NULL, MOSQ_LOG_DEBUG, "\t For Subscriber: %s", context->mqtt_cc.incoming_sub_clientid);
}

void get_qos_metrics_helper_func(struct mosquitto *context, const char *key, const char* val)
{
    char* temp = strdup(key);
    if (!temp) {
        return;
    }

    char *value = strtok((char*)val, ",");
    int i = 0; // Index variable for arrays
    
    while (value != NULL) {
        if (strstr(temp, "tasks") != NULL) {
            strcpy(context->mqtt_cc.incoming_tasks[i], value);
        }
        else if (strstr(temp, "Max_Latency") != NULL) {
            context->mqtt_cc.incoming_max_latencies[i] = atof(value);
        }
        else if (strstr(temp, "Accuracy") != NULL) {
            context->mqtt_cc.incoming_accuracy[i] = atof(value);
        }
        else if (strstr(temp, "Min_Frequency") != NULL) {
            context->mqtt_cc.incoming_frequencies[i] = atoi(value);
        }

        value = strtok(NULL, ","); // Get next value
        i++; // Move to next index
    }

    free(temp);
}

void get_qos_metrics(struct mosquitto *context, const char *passed_insub)
{
    char buffer[256];

    // Safe string copy
    snprintf(buffer, sizeof(buffer), "%s", passed_insub);

    // Find first slash
    char *slash = strchr(buffer, '/');
    if (!slash) {
        fprintf(stderr, "Error: No slash found in topic!\n");
        return;
    }

    // Allocate memory for topic substring
    size_t len = slash - buffer + 1;
    char* result = (char*)malloc(len + 1);
    if (!result) {
        fprintf(stderr, "Error: Memory allocation failed\n");
        return;
    }

    strncpy(result, buffer, len);
    result[len] = '\0';

    // Free old incoming topic before assigning
    if (context->mqtt_cc.incoming_topic) {
        free(context->mqtt_cc.incoming_topic);
    }
    context->mqtt_cc.incoming_topic = result;

    // Move remainder of string into buffer safely
    size_t remaining_length = strlen(slash);
    memmove(buffer, slash + 1, remaining_length);
    buffer[remaining_length] = '\0';  

    char* saveptr_for_semicolons;
    char * pair = strtok_r(buffer, ";",&saveptr_for_semicolons);
    while (pair !=NULL){
        char * saveptr_for_equals;
        char* key = strtok_r(pair, "=", &saveptr_for_equals);
        char *value = strtok_r(NULL, "=",&saveptr_for_equals);
        if (key && value){
            get_qos_metrics_helper_func(context,key,value);
        }
        else {
            return;
        }
        pair = strtok_r(NULL, ";",&saveptr_for_semicolons);
    }
    
    return;

}

char *concat_strings(char *str1, char *str2) {
    // Allocate memory for the concatenated string
    size_t len1 = strlen(str1);
    size_t len2 = strlen(str2);
    char *result = malloc(len1 + len2 + 1); // +1 for the null terminator
    if (result == NULL) {
        perror("Memory allocation failed");
        return NULL;
    }

    // Copy str1 and str2 into the result buffer
    strcpy(result, str1);
    strcat(result, str2);

    return result;
}

void prepare_DB() {
    char *err_msg = NULL;
    int rc;
    prototype_db.db_path = "/mnt/c/Users/sala_/OneDrive/Documents/MQTTRESEARCH/mqtt-cc-research/brokers/broker-PIE/mosquitto/db/piedatabase.db";
    // Open database connection /mnt/c/Users/sala_/OneDrive/Documents/MQTTRESEARCH/mqtt-cc-research/brokers/broker-mqtt-cc/mosquitto/db/database.db"
    rc = sqlite3_open(prototype_db.db_path, &prototype_db.db);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Cannot open database: %s\n", sqlite3_errmsg(prototype_db.db));
        sqlite3_close(prototype_db.db);
        return;
    }

    // Create subscribers table
    const char *create_subscribers_table = 
    "CREATE TABLE IF NOT EXISTS Subscribers ("
    "DeviceMac TEXT PRIMARY KEY, Tasks TEXT, MinFrequency TEXT, MaxAllowedLatency TEXT, Accuracy TEXT);";
    // Create devices table
    const char *create_publishers_table = 
     "CREATE TABLE IF NOT EXISTS Publishers ("
        "DeviceMac TEXT PRIMARY KEY, Accuracy TEXT, Tasks TEXT, MaxFreq TEXT, Energy TEXT, MaxLatency TEXT);";
    // Create topics table
    const char *create_topics_table = 
"CREATE TABLE IF NOT EXISTS Topics ("
        "DeviceMac TEXT, TopicName TEXT, Publishing BOOLEAN, "
        "PRIMARY KEY(DeviceMac, TopicName, Publishing));";

    rc = sqlite3_exec(prototype_db.db, create_subscribers_table, 0, 0, &err_msg);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to create subscribers table: %s\n", err_msg);
        sqlite3_free(err_msg);
    }

    rc = sqlite3_exec(prototype_db.db, create_publishers_table, 0, 0, &err_msg);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to create devices table: %s\n", err_msg);
        sqlite3_free(err_msg);
    }

    rc = sqlite3_exec(prototype_db.db, create_topics_table, 0, 0, &err_msg);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to create topics table: %s\n", err_msg);
        sqlite3_free(err_msg);
    } 
    printf("Created Tables\n");

    const char* insert_topic_cmd = "INSERT INTO Topics (DeviceMac, TopicName, Publishing) VALUES (?, ?, ?);";
    rc = sqlite3_prepare_v2(prototype_db.db, insert_topic_cmd, -1, &prototype_db.insert_topic, 0);
    if (rc != SQLITE_OK) {
        log__printf(NULL, MOSQ_LOG_ERR, "Failed to prepare statement 1: %s\n", sqlite3_errmsg(prototype_db.db));
        sqlite3_close(prototype_db.db);
        exit(1);
    }
    const char* find_whatever_cmd = "SELECT * FROM Topics WHERE TopicName = ?;";
    rc = sqlite3_prepare_v2(prototype_db.db, find_whatever_cmd, -1, &prototype_db.find_whatever,0);
    if (rc != SQLITE_OK) {
        log__printf(NULL, MOSQ_LOG_ERR, "Failed to prepare statement 1: %s\n", sqlite3_errmsg(prototype_db.db));
        sqlite3_close(prototype_db.db);
        exit(1);
    }
    const char* insert_subscribers_cmd = "INSERT INTO Subscribers (DeviceMac,Tasks,MinFrequency,MaxAllowedLatency,Accuracy) VALUES (?, ?, ?, ?, ?);";
    rc = sqlite3_prepare_v2(prototype_db.db, insert_subscribers_cmd, -1, &prototype_db.insert_subscribers,0);
    if (rc != SQLITE_OK) {
        log__printf(NULL, MOSQ_LOG_ERR, "Failed to prepare statement 1: %s\n", sqlite3_errmsg(prototype_db.db));
        sqlite3_close(prototype_db.db);
        exit(1);
    }
    const char* insert_into_publishers_cmd = "INSERT INTO Publishers (DeviceMac, Accuracy, Tasks, MaxFreq, Energy, MaxLatency) VALUES (?,?,?,?,?,?);";
    rc = sqlite3_prepare_v2(prototype_db.db, insert_into_publishers_cmd, -1, &prototype_db.insert_into_publishers,0);
    if (rc != SQLITE_OK) {
        log__printf(NULL, MOSQ_LOG_ERR, "Failed to prepare statement 5: %s\n", sqlite3_errmsg(prototype_db.db));
        sqlite3_close(prototype_db.db);
        exit(1);
    }
}

// Function to insert into the topics table

bool topic_search(struct mosquitto *context){
    int rc;
    log__printf(NULL, MOSQ_LOG_DEBUG, "\ Checking for topic");


    sqlite3_bind_text(prototype_db.find_whatever, 1, (const char*)context->mqtt_cc.incoming_topic, -1, SQLITE_STATIC);

    rc = sqlite3_step(prototype_db.find_whatever);
    log__printf(NULL, MOSQ_LOG_DEBUG, "\ Print successful");
    sqlite3_finalize(prototype_db.find_whatever);
    if(rc == SQLITE_ROW){ // 100
        //printStmtResults(prototype_db.insert_topic);
        //rc2 = sqlite3_reset(prototype_db.insert_topic);
        // DO NOT RESET, since there is a row in the insert_topic statement
        // RESET will be done in update_latency_req_max_allowed
        return true;
    }
    else{return false;}
}


char* create_latency_str(char *clientid, int latencyNum){
    cJSON *json = cJSON_CreateObject();
    cJSON_AddNumberToObject(json, clientid, latencyNum);
    return cJSON_Print(json);
}

void insert_into_topics_table(struct mosquitto *context){
    int rc;
    log__printf(NULL, MOSQ_LOG_INFO, context->mqtt_cc.incoming_topic);
    char * temp = context->mqtt_cc.incoming_topic;

    log__printf(NULL, MOSQ_LOG_INFO, "I am in insert_into_topics_table");
    sqlite3_bind_text(prototype_db.insert_topic, 2, context->mqtt_cc.incoming_topic, -1, SQLITE_STATIC);//topic name problem sala
    uint8_t len = strlen(temp);
    if (len > 0 && temp[len-1] == '/'){
        temp[len-1] = '\0';
    }
    sqlite3_bind_text(prototype_db.insert_topic, 1,temp, -1, SQLITE_STATIC);//devicemac
    sqlite3_bind_int(prototype_db.insert_topic, 3, 0);  // 1 for true, 0 for false
    rc = sqlite3_step(prototype_db.insert_topic);
    if (rc != SQLITE_DONE) {
        fprintf(stderr, "Failed to insert into Topics: %s\n", sqlite3_errmsg(prototype_db.db));
        sqlite3_finalize(prototype_db.insert_topic);
        return rc;
    }
    sqlite3_finalize(prototype_db.insert_topic);
    //hardcoded part used for only testing no longer needed
    
}

void insert_into_subscribers_table(struct mosquitto* context){
    int rc;
    //
    char * temp = context->mqtt_cc.incoming_topic;
    uint8_t len = strlen(temp);
    if (temp[len-1]=='/'){
        temp[len-1]='\0';
    }
    sqlite3_bind_text(prototype_db.insert_subscribers, 1,temp, -1, SQLITE_STATIC);//topic name
    cJSON * array1= cJSON_CreateArray();
    if (array1 == NULL){
        return NULL;
    }
    int numthings1= 2;//sizeof(context->mqtt_cc.incoming_tasks);//sizeof(context->mqtt_cc.incoming_tasks[0]);
    for (int i =0; i < numthings1; i++){

        cJSON_AddItemToArray(array1,cJSON_CreateString(context->mqtt_cc.incoming_tasks[i]));

    }
    char *jsonString = cJSON_PrintUnformatted(array1);
    cJSON_Delete(array1);
    sqlite3_bind_text(prototype_db.insert_subscribers, 2, jsonString,-1,SQLITE_STATIC);  // 1 for true, 0 for false json for tasks?
    //adding frequencies
    cJSON * array2= cJSON_CreateArray();
    if (array2 == NULL){
        return NULL;
    }
    int numthings2= 2;//sizeof(context->mqtt_cc.incoming_frequencies)/sizeof(context->mqtt_cc.incoming_frequencies[0]);
    for (int i =0; i < numthings2; i++){

        cJSON_AddItemToArray(array2,cJSON_CreateNumber(context->mqtt_cc.incoming_frequencies[i]));

    }
    char *jsonString2 = cJSON_PrintUnformatted(array2);
    cJSON_Delete(array2);
    
    sqlite3_bind_text(prototype_db.insert_subscribers, 3, jsonString2,-1,SQLITE_STATIC);  // 1 for true, 0 for false
    
    
    cJSON * array3= cJSON_CreateArray();
    if (array3 == NULL){
        return NULL;
    }
    int numthings3= 2;//sizeof(context->mqtt_cc.incoming_max_latencies)/sizeof(context->mqtt_cc.incoming_max_latencies[0]);
    for (int i =0; i < numthings3; i++){

        cJSON_AddItemToArray(array3,cJSON_CreateNumber(context->mqtt_cc.incoming_max_latencies[i]));

    }
    char *jsonString3 = cJSON_PrintUnformatted(array3);
    cJSON_Delete(array3);
    sqlite3_bind_text(prototype_db.insert_subscribers, 4, jsonString3, -1,SQLITE_STATIC);  // 1 for true, 0 for false

    cJSON * array4= cJSON_CreateArray();
    if (array4 == NULL){
        return NULL;
    }
    int numthings4= 2;//sizeof(context->mqtt_cc.incoming_accuracy)/sizeof(context->mqtt_cc.incoming_accuracy[0]);
    for (int i =0; i < numthings4; i++){

        cJSON_AddItemToArray(array4,cJSON_CreateNumber(context->mqtt_cc.incoming_accuracy[i]));

    }
    char *jsonString4 = cJSON_PrintUnformatted(array4);
    cJSON_Delete(array4);
    sqlite3_bind_text(prototype_db.insert_subscribers, 5, jsonString4, -1,SQLITE_STATIC);

    rc = sqlite3_step(prototype_db.insert_subscribers);
    if (rc != SQLITE_DONE) {
        fprintf(stderr, "Failed to insert into Topics: %s\n", sqlite3_errmsg(prototype_db.db));
        sqlite3_finalize(prototype_db.insert_subscribers);
        return ;
    }
    sqlite3_reset(prototype_db.insert_subscribers);
    sqlite3_finalize(prototype_db.insert_subscribers);

}

void insert_into_publisher_table(struct mosquitto * context){
    int rc;
    //
    char * temp = context->mqtt_cc.incoming_topic;
    uint8_t len = strlen(temp);
    if (temp[len-1]=='/'){
        temp[len-1]='\0';
    }
    sqlite3_bind_text(prototype_db.insert_into_publishers, 1,temp, -1, SQLITE_STATIC);//devicemac
    cJSON * array1= cJSON_CreateArray();
    if (array1 == NULL){
        return NULL;
    }
    int numthings1= 2;//sizeof(context->mqtt_cc.incoming_tasks);//sizeof(context->mqtt_cc.incoming_tasks[0]);
    for (int i =0; i < numthings1; i++){

        cJSON_AddItemToArray(array1,cJSON_CreateNumber(context->mqtt_cc.incoming_accuracy[i]));

    }
    char *jsonString = cJSON_PrintUnformatted(array1);
    cJSON_Delete(array1);
    sqlite3_bind_text(prototype_db.insert_into_publishers, 2, jsonString,-1,SQLITE_STATIC);  // 1 for true, 0 for false json for tasks?
    //adding frequencies
    cJSON * array2= cJSON_CreateArray();
    if (array2 == NULL){
        return NULL;
    }
    int numthings2= 2;//sizeof(context->mqtt_cc.incoming_frequencies)/sizeof(context->mqtt_cc.incoming_frequencies[0]);
    for (int i =0; i < numthings2; i++){

        cJSON_AddItemToArray(array2,cJSON_CreateString(context->mqtt_cc.incoming_tasks[i]));

    }
    char *jsonString2 = cJSON_PrintUnformatted(array2);
    cJSON_Delete(array2);
    
    sqlite3_bind_text(prototype_db.insert_into_publishers, 3, jsonString2,-1,SQLITE_STATIC);  // 1 for true, 0 for false
    
    
    cJSON * array3= cJSON_CreateArray();
    if (array3 == NULL){
        return NULL;
    }
    int numthings3= 2;//sizeof(context->mqtt_cc.incoming_max_latencies)/sizeof(context->mqtt_cc.incoming_max_latencies[0]);
    for (int i =0; i < numthings3; i++){

        cJSON_AddItemToArray(array3,cJSON_CreateNumber(context->mqtt_cc.incoming_frequencies[i]));

    }
    char *jsonString3 = cJSON_PrintUnformatted(array3);
    cJSON_Delete(array3);
    sqlite3_bind_text(prototype_db.insert_into_publishers, 4, jsonString3, -1,SQLITE_STATIC);  // 1 for true, 0 for false

    cJSON * array4= cJSON_CreateArray();
    if (array4 == NULL){
        return NULL;
    }
    int numthings4= 2;//sizeof(context->mqtt_cc.incoming_accuracy)/sizeof(context->mqtt_cc.incoming_accuracy[0]);
    for (int i =0; i < numthings4; i++){

        cJSON_AddItemToArray(array4,cJSON_CreateNumber(context->mqtt_cc.incoming_energy[i]));

    }
    char *jsonString4 = cJSON_PrintUnformatted(array4);
    cJSON_Delete(array4);
    sqlite3_bind_text(prototype_db.insert_into_publishers, 5, jsonString4, -1,SQLITE_STATIC);

    cJSON * array5= cJSON_CreateArray();
    if (array5 == NULL){
        return NULL;
    }
    int numthings5= 2;//sizeof(context->mqtt_cc.incoming_accuracy)/sizeof(context->mqtt_cc.incoming_accuracy[0]);
    for (int i =0; i < numthings5; i++){

        cJSON_AddItemToArray(array5,cJSON_CreateNumber(context->mqtt_cc.incoming_max_latencies[i]));

    }
    char *jsonString5 = cJSON_PrintUnformatted(array4);
    cJSON_Delete(array5);
    sqlite3_bind_text(prototype_db.insert_into_publishers, 6, jsonString5, -1,SQLITE_STATIC);

    rc = sqlite3_step(prototype_db.insert_into_publishers);
    if (rc != SQLITE_DONE) {
        fprintf(stderr, "Failed to insert into Topics: %s\n", sqlite3_errmsg(prototype_db.db));
        sqlite3_finalize(prototype_db.insert_into_publishers);
        return ;
    }
    sqlite3_reset(prototype_db.insert_into_publishers);
    sqlite3_finalize(prototype_db.insert_into_publishers);


}

void insert_topic_in_DB(struct mosquitto *context){
    log__printf(NULL, MOSQ_LOG_INFO, "I am in insert_topic_in_DB");
    log__printf(NULL, MOSQ_LOG_INFO, context->mqtt_cc.incoming_topic);
    int rc;
    int rc2; 
    //pthread_t mess_client;
	//pthread_attr_t mess_client_attr;
    // create latency column value
    char *latencyJsonString = create_latency_str(context->mqtt_cc.incoming_sub_clientid, context->mqtt_cc.incoming_lat_qos);
    //(subscription TEXT PRIMARY KEY, latency_req TEXT, max_allowed_latency INTEGER, added INTEGER, lat_change INTEGER)
    //bind topic and latency to prepared statement 
    sqlite3_bind_text(prototype_db.insert_new_topic, 1, context->mqtt_cc.incoming_topic, -1, SQLITE_STATIC);
    sqlite3_bind_text(prototype_db.insert_new_topic, 2, latencyJsonString, -1, SQLITE_STATIC);
    sqlite3_bind_int(prototype_db.insert_new_topic, 3, context->mqtt_cc.incoming_lat_qos);
    sqlite3_bind_int(prototype_db.insert_new_topic, 4, 1);
    sqlite3_bind_int(prototype_db.insert_new_topic, 5, 0);
    //execute statement
  
    rc = sqlite3_step(prototype_db.insert_new_topic);
	sleep(2);
    //check for error 
    if (rc != SQLITE_DONE) {
        log__printf(NULL, MOSQ_LOG_ERR, "Failed to execute statement: %s\n", sqlite3_errmsg(prototype_db.db));
        sqlite3_close(prototype_db.db);
        exit(1);
    }

    rc2 = sqlite3_reset(prototype_db.insert_new_topic);
    if(rc2 != SQLITE_OK){
        log__printf(NULL, MOSQ_LOG_ERR, "Failed to reset insert_new_topic: %s\n", sqlite3_errmsg(prototype_db.db));
        exit(1);
    }
    log__printf(NULL, MOSQ_LOG_ERR, "Reset insert_new_topic\n");

    
    log__printf(NULL, MOSQ_LOG_DEBUG, "Success: Added topic, latency_req, and max_allowed_latency to DB\n");

   // log__printf(NULL, MOSQ_LOG_DEBUG, "\ In has_lat_qos, topic = %s", context->mqtt_cc.incoming_topic);
    // pthread_attr_init(&mess_client_attr);
	// pthread_attr_setdetachstate(&mess_client_attr, PTHREAD_CREATE_DETACHED);
	// log__printf(NULL, MOSQ_LOG_DEBUG, "\ In has_lat_qos, topic = %s", context->mqtt_cc.incoming_topic);
	// pthread_create(&mess_client, &mess_client_attr, messageClient, (void*)context);
	// pthread_attr_destroy(&mess_client_attr);
}


int calc_new_max_latency(struct cJSON *latencies){
    int numLatencies = 0;
    cJSON *child = latencies->child;
    while(child != NULL){
        numLatencies++;
        child = child->next;
    }
    int *arr = (int *)malloc(numLatencies *sizeof(int));
    
    if (arr == NULL) {
        printf("Memory allocation failed\n");
        exit(1);
    }

    int i = 0;
    cJSON_ArrayForEach(child, latencies){
        //log__printf(NULL, MOSQ_LOG_DEBUG, "Adding latency to int array %ds\n in index %d", child->valueint, i);
        arr[i] = child->valueint;
        i++; 
    }
    //log__printf(NULL, MOSQ_LOG_DEBUG, "Outside array for each");

    int min = arr[0];
    //log__printf(NULL, MOSQ_LOG_DEBUG, "just set min");

    for(i = 1; i < numLatencies; i++){
        //log__printf(NULL, MOSQ_LOG_DEBUG, "entered for loop");

        if(arr[i] < min){
            //log__printf(NULL, MOSQ_LOG_DEBUG, "entered if");

            min = arr[i];
        }
    }
    return min;
}


void update_lat_req_max_allowed(struct mosquitto *context){
    // at this point, the topic does exist in the DB, and the insert_topic stmt contains the row
    int rc;
    int ret;
    pthread_t mess_client;
	pthread_attr_t mess_client_attr;
    // get the old latency value from column 1 (latencyReq)
    
    char *oldLatencyValue = sqlite3_column_text(prototype_db.insert_topic, 1);
    int oldMaxAllowed = sqlite3_column_int(prototype_db.insert_topic, 2);
    
    //log__printf(NULL, MOSQ_LOG_DEBUG, "old Latency Value: %s\n", oldLatencyValue);

    // convert old latency value to json
    cJSON *db_Value = cJSON_Parse(oldLatencyValue);
    // error checking db_Value
    if (db_Value == NULL) { 
        const char *error_ptr = cJSON_GetErrorPtr(); 
        if (error_ptr != NULL) { 
            log__printf(NULL, MOSQ_LOG_DEBUG, "Error: %s\n", error_ptr);
        } 
        log__printf(NULL, MOSQ_LOG_DEBUG, "Exiting Program with db_Value == NULL \n");
        cJSON_Delete(db_Value); 
        exit(1); 
    }
    log__printf(NULL, MOSQ_LOG_DEBUG, "Adding clientid %s and latQos %d to topic %s \n", context->mqtt_cc.incoming_sub_clientid, context->mqtt_cc.incoming_lat_qos, context->mqtt_cc.incoming_topic);


    // calculate the new max allowed latency from 
    // context->mqtt_cc.incoming_lat_qos + the row's existing latencies

    // add new item (clientid: latencyNum) to make new latency value
    cJSON_AddNumberToObject(db_Value, context->mqtt_cc.incoming_sub_clientid, context->mqtt_cc.incoming_lat_qos);

    int newMaxAllowed = calc_new_max_latency(db_Value);
    int lat_changed = 0;
    if (oldMaxAllowed != newMaxAllowed){
        lat_changed = 1;
        // if there is a change in the max_allowed_latency, notify client
        // some time for the client to finish their operation
        // pthread_attr_init(&mess_client_attr);
		// pthread_attr_setdetachstate(&mess_client_attr, PTHREAD_CREATE_DETACHED);
		// pthread_create(&mess_client, &mess_client_attr, messageClient, (void*)context);
		// pthread_attr_destroy(&mess_client_attr);
    }

    // convert new latency value back into string
    char *newLatencyValue = cJSON_Print(db_Value);
    
    //log__printf(NULL, MOSQ_LOG_DEBUG, "new Latency Value: %s\n", newLatencyValue);
    //log__printf(NULL, MOSQ_LOG_DEBUG, "new max allowed latency: %d\n", newMaxAllowed);

    // bind topic and new latency value to update statement
    //(subscription TEXT PRIMARY KEY, latency_req TEXT, max_allowed_latency INTEGER, added INTEGER, lat_change INTEGER)
    sqlite3_bind_text(prototype_db.update_latency_req_max_allowed, 1, newLatencyValue, -1, SQLITE_STATIC);
    sqlite3_bind_int(prototype_db.update_latency_req_max_allowed, 2, newMaxAllowed);
    sqlite3_bind_text(prototype_db.update_latency_req_max_allowed, 4, context->mqtt_cc.incoming_topic, -1, SQLITE_STATIC);
    sqlite3_bind_int(prototype_db.update_latency_req_max_allowed, 3, lat_changed);
    
    // step update statement    
    rc = sqlite3_step(prototype_db.update_latency_req_max_allowed);
    sleep(2);
    // check if statement is DONE
    if (rc != SQLITE_DONE) {
        log__printf(NULL, MOSQ_LOG_ERR, "Failed to execute statement: %s\n", sqlite3_errmsg(prototype_db.db));
        sqlite3_close(prototype_db.db);
        exit(1);
    }

    log__printf(NULL, MOSQ_LOG_DEBUG, "Success: Added Latency Req %d to topic %s for client %s\n", context->mqtt_cc.incoming_lat_qos, context->mqtt_cc.incoming_topic, context->mqtt_cc.incoming_sub_clientid);

        // reset update stmt

    rc = sqlite3_reset(prototype_db.update_latency_req_max_allowed);
    

    // check if reset update stmt was good
    if(rc != SQLITE_OK){
        log__printf(NULL, MOSQ_LOG_ERR, "Failed to reset update statement: %s\n", sqlite3_errmsg(prototype_db.db));
        exit(1);
    }

    //log__printf(NULL, MOSQ_LOG_ERR, "Reset update_latency_req_max_allowed \n");

    // reset find stmt 

    rc = sqlite3_reset(prototype_db.insert_topic);

    // check if reset find stmt was good 
    if(rc != SQLITE_OK){
        log__printf(NULL, MOSQ_LOG_ERR, "Failed to reset find statement: %s\n", sqlite3_errmsg(prototype_db.db));
        exit(1);
    }

    //log__printf(NULL, MOSQ_LOG_ERR, "Reset insert_topic\n");
}
 
    