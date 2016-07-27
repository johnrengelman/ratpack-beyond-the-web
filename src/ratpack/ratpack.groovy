import com.ecwid.consul.v1.ConsulClient
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.google.common.io.ByteSource
import com.google.common.io.Resources
import com.rabbitmq.client.AMQP
import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.Consumer
import com.rabbitmq.client.Envelope
import com.rabbitmq.client.ShutdownSignalException
import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString
import ratpack.config.ConfigSource
import ratpack.config.internal.source.YamlConfigSource
import ratpack.exec.*
import ratpack.file.FileSystemBinding
import ratpack.func.Action
import ratpack.service.Service
import ratpack.service.StartEvent
import ratpack.service.StopEvent

import com.rabbitmq.client.Channel
import java.util.concurrent.TimeUnit

import static ratpack.groovy.Groovy.ratpack

import asset.pipeline.ratpack.AssetPipelineHandler
import asset.pipeline.ratpack.AssetPipelineModule

ratpack {
    serverConfig {
//        json('assets.json')

        yaml(Resources.asByteSource(Resources.getResource("application.yaml")))
        // tag::custom-source[]
        add new ConfigSource() {
            @Override
            ObjectNode loadConfigData(ObjectMapper objectMapper, FileSystemBinding fileSystemBinding) throws Exception {
                ConsulClient consul = new ConsulClient()
                String config = consul.getKVValue("/demo").value.decodedValue
                YamlConfigSource yaml = new YamlConfigSource(ByteSource.wrap(config.bytes))
                ObjectNode root = objectMapper.createObjectNode()
                root.set("demo", yaml.loadConfigData(objectMapper, fileSystemBinding))
                return root
            }
        }
        // end::custom-source[]
        require("/demo", DemoConfig)
    }
    bindings {
        multiBindInstance Service.startup("config") { StartEvent event ->
            println event.registry.get(DemoConfig)
        }
        multiBindInstance Service.startup("cron") { StartEvent event ->
            ExecController.require().executor.scheduleAtFixedRate({
                Execution.fork().register {
                    it.add new YAMLFactory(event.registry.get(ObjectMapper))
                    it.add new ConsulClient()
                    it.add event.registry.get(DemoConfig)
                }.start({ Execution e ->
                    println "Checking for modified application secrets"
                    Blocking.get {
                        e.get(ConsulClient).getKVValue("/demo")
                    }.map {
                        it.value.decodedValue
                    }.map {
                        e.get(YAMLFactory).createParser(it)
                    }.map {
                        it.readValueAs(DemoConfig)
                    }.then {
                        if (it.secrets != e.get(DemoConfig).secrets) {
                            println "ERROR: application secret values have changed"
                        }
                    }
                } as Action<Execution>)
            } as Runnable, 5, 5, TimeUnit.SECONDS)
        }
        multiBindInstance new RabbitService("demo")
        moduleConfig(AssetPipelineModule, serverConfig.get("/assets", AssetPipelineModule.Config))
    }
    handlers {
        all(AssetPipelineHandler)
    }
}

@ToString
class DemoConfig {
    String name
    List<String> people
    Secret secrets
}

@ToString
@EqualsAndHashCode
class Secret {
    String key
    String secret
    SuperSecret supersecret
}

@ToString
@EqualsAndHashCode
class SuperSecret {
    String hidden
}

class RabbitService implements Service {

    String queue
    Connection connection
    ExecController controller

    RabbitService(String queue) {
        this.queue = queue
        this.controller = ExecController.require()
        ConnectionFactory cf = new ConnectionFactory()
        connection = cf.newConnection()

    }

    @Override
    void onStart(StartEvent event) throws Exception {
        Execution.fork().start {
            Channel channel = connection.createChannel()
            channel.queueBind(queue, queue, "")
            channel.basicConsume(queue, new Consumer() {
                @Override
                void handleConsumeOk(String consumerTag) {

                }

                @Override
                void handleCancelOk(String consumerTag) {

                }

                @Override
                void handleCancel(String consumerTag) throws IOException {

                }

                @Override
                void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {

                }

                @Override
                void handleRecoverOk(String consumerTag) {

                }

                @Override
                void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    controller.fork().start {
                        println "Received Rabbit Message"
                        println "Contents: ${new String(body)}"
                    }

                }
            })
        }
    }

    @Override
    void onStop(StopEvent event) throws Exception {
        connection.close()
    }
}