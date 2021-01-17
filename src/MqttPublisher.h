#ifndef MQTT_PUBLISHER_H
#define MQTT_PUBLISHER_H

#include "config.h"
#include "debug.h"
#include "MQTT.h"
#include <string.h>
#include <sml/sml_file.h>
#include <TimeLib.h>

struct MqttConfig
{
  char server[128] = "mosquitto";
  char port[8] = "1883";
  char username[128] = "";
  char password[128] = "";
  char topic[128] = "iot/smartmeter/";
};

class MqttPublisher
{
public:
  void setup(MqttConfig _config)
  {
    DEBUG("Setting up MQTT publisher.");
    config = _config;
    uint8_t lastCharOfTopic = strlen(config.topic) - 1;
    baseTopic = String(config.topic) + (lastCharOfTopic >= 0 && config.topic[lastCharOfTopic] == '/' ? "" : "/");

    client.begin(config.server, atoi(config.port), net);
  }

  void connect()
  {
    DEBUG("Establishing MQTT client connection.");
    client.connect("SMLReader", config.username, config.password);
    if (client.connected())
    {
      char message[64];
      snprintf(message, 64, "Hello from %08X, running SMLReader version %s.", ESP.getChipId(), VERSION);
      info(message);
    }
  }

  void loop()
  {
    client.loop();
  }

  void debug(const char *message)
  {
    publish(baseTopic + "debug", message);
  }

  void info(const char *message)
  {
    publish(baseTopic + "info", message);
  }

  void publish(Sensor *sensor, sml_file *file)
  {

    for (int i = 0; i < file->messages_len; i++)
    {
        sml_message *message = file->messages[i];
        if (*message->message_body->tag == SML_MESSAGE_GET_LIST_RESPONSE)
        {
            sml_list *entry;
            sml_get_list_response *body;
            body = (sml_get_list_response *)message->message_body->data;
            for (entry = body->val_list; entry != NULL; entry = entry->next)
            {
                if (!entry->value)
                {   // do not crash on null value
                    continue;
                }

                char obisIdentifier[32];
                char OID[32];
                char buffer[255];

                sprintf(OID, "%d.%d.%d",
                        entry->obj_name->str[2], entry->obj_name->str[3],
                        entry->obj_name->str[4]);

                String OIDs = OID;

                if (OIDs.equals("1.8.0") || OIDs.equals("2.8.0") || OIDs.equals("16.7.0")) {
                  if (OIDs.equals("1.8.0")) {
                    OIDs = "Total_in";
                  }
                  else if (OIDs.equals("2.8.0")) {
                    OIDs = "Total_out";
                  }
                  else if (OIDs.equals("16.7.0")) {
                    OIDs = "Power_curr";
                  }

                if (((entry->value->type & SML_TYPE_FIELD) == SML_TYPE_INTEGER) ||
                         ((entry->value->type & SML_TYPE_FIELD) == SML_TYPE_UNSIGNED))
                {
                    double value = sml_value_to_double(entry->value);
                    int scaler = (entry->scaler) ? *entry->scaler : 0;
                    int prec = -scaler;
                    if (prec < 0)
                        prec = 0;
                    value = value * pow(10, scaler);
                    if (OIDs.startsWith("Total_")) {
                      value = value / 1000.0;
                    }
                    sprintf(buffer, "%.*f", prec, value);
                    char ts[24];
                    sprintf(ts, "%4d-%02d-%02dT%02d:%02d:%02d", 
                        year(timeClient.getEpochTime()), 
                        month(timeClient.getEpochTime()), 
                        day(timeClient.getEpochTime()), 
                        hour(timeClient.getEpochTime()), 
                        minute(timeClient.getEpochTime()), 
                        second(timeClient.getEpochTime()));
                    String tss = ts;
                    String payload = "{\"Time\":\"" + tss + "\",\"ENERGY\":{\"" + OIDs + "\":" + value + "}}";
                    DEBUG(payload.c_str());
                    publish("tele/tasmota_SMLSML/SENSOR", payload);
                }
                else if (!sensor->config->numeric_only) {
                  if (entry->value->type == SML_TYPE_OCTET_STRING)
                  {
                      char *value;
                      sml_value_to_strhex(entry->value, &value, true);
                      char ts[24];
                      sprintf(ts, "%4d-%02d-%02dT%02d:%02d:%02d", 
                          year(timeClient.getEpochTime()), 
                          month(timeClient.getEpochTime()), 
                          day(timeClient.getEpochTime()), 
                          hour(timeClient.getEpochTime()), 
                          minute(timeClient.getEpochTime()), 
                          second(timeClient.getEpochTime()));
                      String tss = ts;
                      String payload = "{\"Time\":\"" + tss + "\",\"ENERGY\":{\"" + OIDs + "\":" + buffer + "}}";
                      DEBUG(payload.c_str());
                      publish("tele/tasmota_SMLSML/SENSOR", payload);
                      free(value);
                  }
                  else if (entry->value->type == SML_TYPE_BOOLEAN)
                  {
                    char ts[24];
                    sprintf(ts, "%4d-%02d-%02dT%02d:%02d:%02d", 
                        year(timeClient.getEpochTime()), 
                        month(timeClient.getEpochTime()), 
                        day(timeClient.getEpochTime()), 
                        hour(timeClient.getEpochTime()), 
                        minute(timeClient.getEpochTime()), 
                        second(timeClient.getEpochTime()));
                    String tss = ts;
                    String payload = "{\"Time\":\"" + tss + "\",\"ENERGY\":{\"" + OIDs + "\":" + (entry->value->data.boolean ? "true" : "false") + "}}";
                    DEBUG(payload.c_str());
                    publish("tele/tasmota_SMLSML/SENSOR", payload);
                  }
                }
            }
            }
        }
    }
  }

private:
  MqttConfig config;
  WiFiClient net;
  MQTTClient client = MQTTClient(512);
  String baseTopic;

  void publish(const String &topic, const String &payload)
  {
    publish(topic.c_str(), payload.c_str());
  }
  void publish(String &topic, const char *payload)
  {
    publish(topic.c_str(), payload);
  }
  void publish(const char *topic, const String &payload)
  {
    publish(topic, payload.c_str());
  }
  void publish(const char *topic, const char *payload)
  {
    if (!client.connected())
    {
      connect();
    }
    if (!client.connected())
    {
      // Something failed
      DEBUG("Connection to MQTT broker failed.");
      DEBUG("Unable to publish a message to '%s'.", topic);
      return;
    }
    DEBUG("Publishing message to '%s':", topic);
    DEBUG("%s\n", payload);
    client.publish(topic, payload);
  }
};

#endif