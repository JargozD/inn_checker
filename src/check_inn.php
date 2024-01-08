<?php
require __DIR__ . '/../vendor/autoload.php';

use Dadata\DadataClient;
use RdKafka\Conf;
use RdKafka\KafkaConsumer;

/**
 * Класс получает из очереди Kafka список ИНН для обработки.
 * Для каждого ИНН производится обращение в сервис Dadata для получения данных о юр.лице, 
 * которые сохраняются в файл `./inns/{ИНН}.txt`
 */
class CheckInn
{
    private KafkaConsumer $consumer;

    private DadataClient $dadata;

    public function __construct()
    {
        $conf = new Conf();
        $conf->set('metadata.broker.list', getenv('KAFKA_CONN'));
        $conf->set('group.id', getenv('KAFKA_GROUP_NAME'));
        $conf->set('auto.offset.reset', 'earliest');
        $conf->set('enable.partition.eof', 'true');

        $this->consumer = new KafkaConsumer($conf);
        $this->consumer->subscribe([getenv('KAFKA_TOPIC_NAME')]);

        $this->dadata = new DadataClient(
            getenv('DADATA_API_KEY'),
            getenv('DADATA_SECRET_KEY')
        );
    }

    public function __destruct()
    {
        $this->consumer->close();
    }

    /**
     * Обработка данных из очереди
     */
    public function execute()
    {
        echo "Начало обработки данных" . PHP_EOL;

        $count = 0;

        while ($msg = $this->consumer->consume(5000)) {
            switch ($msg->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    if (!empty($msg->payload)) {
                        $data = $this->getData($msg->payload);
                        if ($data) {
                            $this->saveData($msg->payload, $data);
                            $count++;

                            echo "Данные по ИНН успешно записаны в файл '$msg->payload'.\n";
                        }
                    }
                    break;
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    break 2;
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    echo "Тайм-аут\n";
                    break;
                default:
                    throw new \Exception($msg->errstr(), $msg->err);
                    break;
            }
        }

        echo "Обработано $count сообщений." . PHP_EOL;
    }

    /**
     * Получает данные из сервиса Dadata по ИНН юридического лица
     */
    private function getData($inn)
    {
        $result = $this->dadata->suggest('party', $inn, 1, ['type' => 'LEGAL']);

        return $result ?? [];
    }

    /**
     * Сохраняет данные по ИНН в файл ./inns/{ИНН}.txt
     */
    private function saveData($inn, $data)
    {
        $fileName = __DIR__ . '/../inns/' . $inn . ".txt";
        file_put_contents($fileName, json_encode($data, JSON_UNESCAPED_UNICODE | JSON_PRETTY_PRINT));
    }
}


$obj = new CheckInn();
$obj->execute();
