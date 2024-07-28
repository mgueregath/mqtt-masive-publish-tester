import time
import paho.mqtt.client as mqtt
from datetime import datetime
import json



def on_publish(client, userdata, mid, reason_code, properties):
    # reason_code and properties will only be present in MQTTv5. It's always unset in MQTTv3
    try:
        userdata.remove(mid)
    except KeyError:
        print("on_publish() is called with a mid not present in unacked_publish")
        print("This is due to an unavoidable race-condition:")
        print("* publish() return the mid of the message sent.")
        print("* mid from publish() is added to unacked_publish by the main thread")
        print("* on_publish() is called by the loop_start thread")
        print("While unlikely (because on_publish() will be called after a network round-trip),")
        print(" this is a race-condition that COULD happen")
        print("")
        print("The best solution to avoid race-condition is using the msg_info from publish()")
        print("We could also try using a list of acknowledged mid rather than removing from pending list,")
        print("but remember that mid could be re-used !")

unacked_publish = set()
mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
mqttc.on_publish = on_publish
mqttc.username_pw_set('bugs', 'bunny')

mqttc.user_data_set(unacked_publish)
mqttc.connect("localhost", 13883)
mqttc.loop_start()

start = datetime.now()

for t in range(0, 50): 
    data = {
        "root": {
            "varname": "test mqtt set " + str(t),
            "date": datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'),
            "boolVar": False
        }
    }
    json_data = json.dumps(data)
    for i in range(0, 392):
        # Our application produce some messages
        msg_info = mqttc.publish("hpc/test/topic/" + str(i), json_data, qos=0)
        unacked_publish.add(msg_info.mid)
    time.sleep(0.2)

end = datetime.now()
print("date and time =", start.strftime('%Y-%m-%d %H:%M:%S.%f'))
print("date and time =", end.strftime('%Y-%m-%d %H:%M:%S.%f'))
print("Total time: ", end - start)


end = datetime.now()
print("date and time =", start.strftime('%Y-%m-%d %H:%M:%S.%f'))
print("date and time =", end.strftime('%Y-%m-%d %H:%M:%S.%f'))
print("Total time: ", end - start)


mqttc.disconnect()
mqttc.loop_stop()