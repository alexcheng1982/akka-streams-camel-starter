import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.ClosedShape
import akka.stream.FlowShape
import akka.stream.Graph
import akka.stream.javadsl.GraphDSL
import akka.stream.javadsl.RunnableGraph
import akka.stream.javadsl.Source
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Sink
import org.apache.camel.impl.DefaultCamelContext
import streamz.camel.StreamContext
import streamz.camel.akka.javadsl.JavaDsl

abstract class CamelEndpoint(private val streamContext: StreamContext) : JavaDsl {
    abstract fun toUri(): String

    override fun streamContext(): StreamContext {
        return streamContext
    }
}

class CamelFileEndpoint(streamContext: StreamContext, private val path: String) : CamelEndpoint(streamContext) {
    override fun toUri(): String {
        return "file://$path"
    }

    fun receive(): Source<ByteArray, NotUsed> {
        return receiveBody(toUri(), ByteArray::class.java)
    }

    fun send(): Graph<FlowShape<ByteArray, ByteArray>, NotUsed> {
        return sendBody<ByteArray>(toUri())
    }
}

fun main(args: Array<String>) {
    val system = ActorSystem.create("CamelTest")
    val materializer = ActorMaterializer.create(system)
    val camelContext = DefaultCamelContext()
    val streamContext = StreamContext.create(camelContext)

    val fileInput = CamelFileEndpoint(streamContext, "/tmp/akka-input")
    val fileOutput = CamelFileEndpoint(streamContext, "/tmp/akka-output")
    val toUpperCase = Flow.fromFunction<ByteArray, ByteArray> { String(it).toUpperCase().toByteArray() }
    val runnableGraph = RunnableGraph.fromGraph(GraphDSL.create { builder ->
        val toUpperCaseShape = builder.add(toUpperCase)
        val input = builder.add(fileInput.receive())
        val output = builder.add(fileOutput.send())
        val fileSink = builder.add(Sink.ignore())
        builder.from(input.out()).toInlet(toUpperCaseShape.`in`())
        builder.from(toUpperCaseShape.out()).toInlet(output.`in`())
        builder.from(output.out()).toInlet(fileSink.`in`())
        ClosedShape.getInstance()
    })
    runnableGraph.run(materializer)
}