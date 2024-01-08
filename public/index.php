<?php
require __DIR__ . '/../vendor/autoload.php';

use Cryonighter\InnChecksum\InnChecker;
use RdKafka\Producer;

$inn = $_GET['inn'] ?? '';
if (!InnChecker::check($inn)) {
    echo "Неверный ИНН";
    exit();
}

if (strlen($inn) != 10) {
    echo "ИНН не принадлежит юридическому лицу";
    exit();
}

$producer = new Producer();

if ($producer->addBrokers(getenv('KAFKA_CONN')) < 1) {
    echo "Ошибка добавления брокера";
    exit;
}

$topic = $producer->newTopic(getenv('KAFKA_TOPIC_NAME'));

if (!$producer->getMetadata(false, $topic, 2000)) {
    echo "Ошибка получения метаданных, брокер упал?";
    exit;
}

$topic->produce(RD_KAFKA_PARTITION_UA, 0, $inn);

$producer->flush(-1);

echo "Сообщение опубликовано";