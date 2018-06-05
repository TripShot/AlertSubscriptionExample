package com.tripshot.alertsubscription;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.http.json.JsonHttpContent;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.JsonObjectParser;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.Key;
import com.google.api.client.util.Preconditions;
import com.google.api.client.util.escape.CharEscapers;
import org.joda.time.Instant;
import org.joda.time.LocalTime;
import org.joda.time.format.ISODateTimeFormat;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class Main {

  /*
   * Example of a Tripshot api consumer subscribing its users to alerts and managing the alert notifications.
   *
   * This assumes a database with a schema like so:
   *
   * CREATE TABLE alert_subscriptions ( sub_key text not null, user_id text not null, primary key (sub_key, user_id) );
   *
   * This table maps a subscription key (sub_key) to an arbitrary set of consumer defined user ids.
   */

  // Your appId, secret, and baseUrl go here, or better yet in a config file that is read on startup.
  private static final String appId = "00000000-0000-0000-0000-000000000000";
  private static final String secret = "YOUR_SECRET";
  private static final String baseUrl = "https://something.tripshot.com";

  private static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
  private static final JsonFactory JSON_FACTORY = new JacksonFactory();

  // Representation of a userId on your system.
  private static class UserId {
    // ...
  }

  public static class AccessTokenRequest {
    @SuppressWarnings("unused")
    @Key
    private String appId;

    @SuppressWarnings("unused")
    @Key
    private String secret;

    public AccessTokenRequest(String appId, String secret) {
      this.appId = appId;
      this.secret = secret;
    }
  }

  public static class AccessTokenResponse {
    @SuppressWarnings("unused")
    @Key
    private String accessToken;
  }

  // A unique key for a subscription. In this example we only consider approaching alerts for a particular route/stop/scheduleTime with fixed 10 minute trigger.
  // But if more flexible subscriptions are supported, the parameters to fully define that subscription should be added here and 'toString' updated accordingly.
  private static class SubscriptionKey {
    private UUID routeId;
    private UUID stopId;
    private LocalTime scheduledTime;

    SubscriptionKey(UUID routeId, UUID stopId, LocalTime scheduledTime) {
      this.routeId = routeId;
      this.stopId = stopId;
      this.scheduledTime = scheduledTime;
    }

    // The representation of this key used to register with Tripshot, as well as key in local DB for all subscribed users. It must completely disambiguate
    // subscriptions. For example if a user can subscribe to approaching alerts with different trigger times, that must be incorporated into this string form.
    public String toString() {
      return "SK:" + routeId.toString() + ":" + stopId.toString() + ":" + scheduledTime.toString();
    }
  }

  private static class AppRouteSubscription {
    @Key
    private String appId;

    @Key
    private String subKey;

    @SuppressWarnings("unused")
    @Key
    private String routeId;

    @SuppressWarnings("unused")
    @Key
    private String stopId;

    @SuppressWarnings("unused")
    @Key
    private String scheduledTime;

    @SuppressWarnings("unused")
    @Key
    @Nullable
    private Integer triggerApproachingMin;

    AppRouteSubscription(String appId, String subKey, UUID routeId, UUID stopId, LocalTime scheduledTime, @Nullable Integer triggerApproachingMin) {
      this.appId = appId;
      this.subKey = subKey;
      this.routeId = routeId.toString();
      this.stopId = stopId.toString();
      this.scheduledTime = ISODateTimeFormat.hourMinuteSecondFraction().print(scheduledTime);
      this.triggerApproachingMin = triggerApproachingMin;
    }
  }

  public static class ApproachingAlert {
    // TODO: Fill in details for alerts of interest.
  }

  public static class WrappedAlert {
    @SuppressWarnings("unused")
    @Key("ApproachingStopAlert")
    private ApproachingAlert approachingAlert;

    @SuppressWarnings("unused")
    @Key
    private String cursor;

    @SuppressWarnings("unused")
    @Key
    private String subKey;

    @Override
    public String toString() {
      return "WrappedAlert{" +
        "alert=" + approachingAlert +
        ", cursor='" + cursor + '\'' +
        ", subKey='" + subKey + '\'' +
        '}';
    }
  }

  public void subscribeUserToApproachingAlert(HttpRequestFactory requestFactory, UserId userId, UUID routeId, UUID stopId, LocalTime scheduledTime)
    throws IOException {
    SubscriptionKey key = new SubscriptionKey(routeId, stopId, scheduledTime);

    boolean isFirst = usersSubscribed(key.toString()).isEmpty();

    insertSubscription(key.toString(), userId);

    if ( isFirst ) {
      createAppRouteSubscription(requestFactory, new AppRouteSubscription(appId, key.toString(), key.routeId, key.stopId, key.scheduledTime, 10));
    }
  }

  public void unsubscribeUserToApproachingAlert(HttpRequestFactory requestFactory, UserId userId, UUID routeId, UUID stopId, LocalTime scheduledTime)
    throws IOException {
    SubscriptionKey key = new SubscriptionKey(routeId, stopId, scheduledTime);

    deleteSubscription(key.toString(), userId);

    boolean isDone = usersSubscribed(key.toString()).isEmpty();

    if ( isDone ) {
      clearAppRouteSubscription(requestFactory, key.toString());
    }
  }


  private void insertSubscription(String subKey, UserId userId) {
    // TODO: insert a row into DB for ('subKey', 'userId')
  }

  private void deleteSubscription(String subKey, UserId userId) {
    // TODO: delete the row in DB for ('subKey', 'userId')
  }

  private List<UserId> usersSubscribed(String subKey) {
    // TODO: lookup and return list of userIds with a subscription to 'subKey'
    return Collections.emptyList();
  }

  private void watchForAndSendAlerts(HttpRequestFactory requestFactory) throws InterruptedException, IOException {

    String cursor = null;
    Instant startTime = new Instant();

    //noinspection InfiniteLoopStatement
    while ( true ) {
      List<WrappedAlert> alerts;

      if ( cursor != null ) {
        alerts = fetchAlerts(requestFactory, cursor, null);
      } else {
        alerts = fetchAlerts(requestFactory, null, startTime);
      }

      for ( WrappedAlert alert : alerts ) {
        try {
          sendToUser(alert);
        } catch ( Throwable t ) {
          // Just log the problem, but keep on going to allow other alerts to fire.
          t.printStackTrace();
        }
      }

      if ( alerts.isEmpty() ) {
        // If there were no alerts in last poll, back off for a bit ...
        Thread.sleep(5000);
      } else {
        // ... but if there were alerts, try again immediately until there are no more ready.
        cursor = alerts.get(alerts.size() - 1).cursor;
      }
    }
  }

  private void sendToUser(WrappedAlert alert) {

    System.out.println("sending notifications for alert : " + alert);

    // TODO: Filter out any unknown or undesired alerts. A subscription will produce all user-facing alerts, including Approaching, Late, Cancelation, Catchup.
    // This example only considers ApproachingAlerts by requiring non-null ApproachingAlert field in WrappedAlert.
    if ( alert.approachingAlert == null ) {
      return;
    }

    List<UserId> userIds = usersSubscribed(alert.subKey);

    for ( UserId userId : userIds ) {
      // push notification to user with given userId
    }
  }

  private void createAppRouteSubscription(HttpRequestFactory requestFactory, AppRouteSubscription appRouteSubscription) throws IOException {
    String accessToken = getAccessToken(requestFactory);

    HttpRequest request = requestFactory.buildPutRequest(new GenericUrl(
        baseUrl + "/v1/appRouteSubscription/" + appRouteSubscription.appId + "/" + CharEscapers.escapeUriPath(appRouteSubscription.subKey)),
      new JsonHttpContent(JSON_FACTORY, appRouteSubscription));
    request.setHeaders(new HttpHeaders().setAuthorization("Bearer " + accessToken));

    HttpResponse response = request.execute();
    try {
      if ( !response.isSuccessStatusCode() ) {
        throw new RuntimeException(
          "failed to create app subscription, (subscription, status, error) = (" + appRouteSubscription + ", " + response.getStatusCode() + ", " +
            response.parseAsString() + ")");
      }
    } finally {
      response.disconnect();
    }

  }

  private void clearAppRouteSubscription(HttpRequestFactory requestFactory, String subKey) throws IOException {
    String accessToken = getAccessToken(requestFactory);

    HttpRequest request =
      requestFactory.buildDeleteRequest(new GenericUrl(baseUrl + "/v1/appRouteSubscription/" + appId.toString() + "/" + CharEscapers.escapeUriPath(subKey)));
    request.setHeaders(new HttpHeaders().setAuthorization("Bearer " + accessToken));

    HttpResponse response = request.execute();
    try {
      if ( !response.isSuccessStatusCode() ) {
        throw new RuntimeException(
          "failed to delete app subscription, (subKey, status, error) = (" + subKey + ", " + response.getStatusCode() + ", " +
            response.parseAsString() + ")");
      }
    } finally {
      response.disconnect();
    }
  }

  private List<WrappedAlert> fetchAlerts(HttpRequestFactory requestFactory, String cursor, Instant startTime) throws IOException {
    Preconditions.checkArgument((cursor == null) != (startTime == null), "exactly one of 'cursor' or 'startTime' must be non-null");

    // Getting a new token for every request is not efficient as a token can be reused until a 401 is returned. But keeping it simple for the example.
    String accessToken = getAccessToken(requestFactory);

    String url = baseUrl + "/v1/appAlerts/" + appId.toString();
    if ( cursor != null ) {
      url = url + "?cursor=" + CharEscapers.escapeUriPath(cursor);
    } else {
      url = url + "?startTime=" + ISODateTimeFormat.dateHourMinuteSecondFraction().print(startTime);
    }

    HttpRequest request = requestFactory.buildGetRequest(new GenericUrl(url));
    request.setHeaders(new HttpHeaders().setAuthorization("Bearer " + accessToken));

    //noinspection unchecked
    return (List<WrappedAlert>) request.execute().parseAs(new TypeReference<List<WrappedAlert>>(){}.getType());
  }

  private static String getAccessToken(HttpRequestFactory requestFactory) throws IOException {

    HttpRequest accessTokenRequest =
      requestFactory.buildPostRequest(new GenericUrl(baseUrl + "/v1/accessToken"), new JsonHttpContent(JSON_FACTORY, new AccessTokenRequest(appId.toString(), secret)));
    AccessTokenResponse accessTokenResponse = accessTokenRequest.execute().parseAs(AccessTokenResponse.class);

    return accessTokenResponse.accessToken;
  }

  public static void main(String[] args) throws IOException {

    new Main().startup();
  }

  private void startup() throws IOException {
    HttpRequestFactory requestFactory = HTTP_TRANSPORT.createRequestFactory(request -> request.setParser(new JsonObjectParser(JSON_FACTORY)));

    // Start polling in another thread. This is not strictly necessary in this simple example, but in a real app we don't want to block the main thread.
    new Thread(() -> {
      try {
        watchForAndSendAlerts(requestFactory);
      } catch ( InterruptedException | IOException e ) {
        e.printStackTrace();
      }
    }).start();

    // Start some kind of service that allows a client to call 'subscribeUserToApproachingAlert' and 'unsubscribeUserToApproachingAlert'.
  }
}
