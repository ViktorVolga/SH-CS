package messagehandler

import (
	"log"
	"sh-cs/proto"
	"time"

	protobuf "google.golang.org/protobuf/proto"
)

func PrintTemperatureMessage(tMessage proto.TemperatureMessage) {
	log.Println("Received message:")
	log.Println("DeviceId: ", tMessage.GetMDeviceId())
	log.Println("SensorId: ", tMessage.GetMSensorId())
	log.Println("Temperature: ", tMessage.GetMTemperature())
	unixTime := tMessage.GetMTimeStamp()
	tm := time.Unix(int64(unixTime)/1000, 0)
	formattedTime := tm.Format("02-01-2006 15:04:05")
	log.Println("Timestamp: ", formattedTime)
}

func pushToKafka(tMessage proto.TemperatureMessage) {

}

func NewTemperatureMessage(binaryData []byte) (*proto.TemperatureMessage, error) {
	temperatureMessage := proto.TemperatureMessage{}
	err := protobuf.Unmarshal(binaryData, &temperatureMessage)
	if err != nil {
		log.Println("Proto error:", err)
		return nil, err
	}
	PrintTemperatureMessage(temperatureMessage)
	return &temperatureMessage, nil
}
