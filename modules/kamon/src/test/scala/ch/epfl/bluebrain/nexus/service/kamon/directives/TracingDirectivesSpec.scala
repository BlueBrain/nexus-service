package ch.epfl.bluebrain.nexus.service.kamon.directives

import java.util.concurrent.ConcurrentLinkedDeque

import akka.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import ch.epfl.bluebrain.nexus.service.kamon.directives.TracingDirectivesSpec.TestSpanReporter
import com.github.ghik.silencer.silent
import com.typesafe.config.{Config, ConfigFactory}
import kamon.akka.http.AkkaHttp.serverOperationName
import kamon.context.Context
import kamon.trace.Span
import kamon.trace.Span.TagValue
import kamon.util.Registration
import kamon.{Kamon, SpanReporter}
import org.scalatest.concurrent.Eventually
import org.scalatest._

class TracingDirectivesSpec
  extends WordSpecLike
    with Matchers
    with Eventually
    with ScalatestRouteTest
    with BeforeAndAfter
    with BeforeAndAfterAll
    with OptionValues {

  override def testConfig: Config = ConfigFactory.load("test-application.conf").withFallback(ConfigFactory.load())

  private var reporter: TestSpanReporter = _
  private var registration: Registration = _

  before {
    reporter = new TestSpanReporter
    registration = Kamon.addReporter(reporter)
  }

  after {
    Kamon.stopAllReporters()
    registration.cancel()
  }

  "The TracingDirectives" should {
    "add a span tag with the payload" when {
      "matches all filtering criteria" in {
        Post("/", HttpEntity(ContentTypes.`application/json`, "{}")) ~> route() ~> check {
          status shouldEqual StatusCodes.OK
        }
        eventualSpan.tags.get("payload").value shouldEqual TagValue.String("{}")
      }
    }
    "not add a span tag with the payload" when {
      "the content type does not match an expected value" in {
        Post("/", HttpEntity(ContentTypes.`text/plain(UTF-8)`, "{}")) ~> route() ~> check {
          status shouldEqual StatusCodes.OK
        }
        eventualSpan.tags.keys should not contain "payload"
      }

      "the content length is higher than the allowed value" in {
        Post("/", HttpEntity(ContentTypes.`application/json`, "123456")) ~> route() ~> check {
          status shouldEqual StatusCodes.OK
        }
        eventualSpan.tags.keys should not contain "payload"
      }

      "the request does not contain a http entity" in {
        Post("/") ~> route() ~> check {
          status shouldEqual StatusCodes.OK
        }
        eventualSpan.tags.keys should not contain "payload"
      }

      "the include payload is set to false" in {
        Post("/", HttpEntity(ContentTypes.`application/json`, "{}")) ~> route(includePayload = false) ~> check {
          status shouldEqual StatusCodes.OK
        }
        eventualSpan.tags.keys should not contain "payload"
      }
    }
  }

  def route(includePayload: Boolean = true): Route = wrap {
    val tracingDirectives = TracingDirectives()
    import tracingDirectives._
    pathEndOrSingleSlash {
      trace("my-trace", includePayload = includePayload) {
        post {
          Kamon.currentSpan().finish()
          complete("")
        }
      }
    }
  }

  def wrap(route: => Route): Route = {
    Route.seal(
      extractRequest { request =>
        val parentContext = Context.create()
        val span = Kamon.buildSpan(serverOperationName(request))
          .asChildOf(parentContext.get(Span.ContextKey))
          .withMetricTag("span.kind", "server")
          .withTag("component", "akka.http.server")
          .withTag("http.method", request.method.value)
          .withTag("http.url", request.uri.toString())
          .start()

        Kamon.storeContext(parentContext.withKey(Span.ContextKey, span))
        route
      }
    )
  }

  def eventualSpan: Span.FinishedSpan = eventually {
    val reported = reportedSpans()
    reported.size shouldEqual 1
    reported.headOption.value
  }

  def reportedSpans(): Seq[Span.FinishedSpan] = {
    reporter.reportedSpans()
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    Kamon.reconfigure(testConfig)
  }
}

object TracingDirectivesSpec {

  class TestSpanReporter extends SpanReporter {
    import scala.collection.JavaConverters._
    private val spans = new ConcurrentLinkedDeque[Span.FinishedSpan]()

    def clear(): Unit =
      spans.clear()

    def reportedSpans(): Seq[Span.FinishedSpan] =
      spans.asScala.toSeq

    @silent
    override def reportSpans(spans: Seq[Span.FinishedSpan]): Unit = {
      this.spans.addAll(spans.asJava)
    }

    override def start(): Unit = ()
    override def stop(): Unit = ()
    override def reconfigure(config: Config): Unit = ()
  }
}