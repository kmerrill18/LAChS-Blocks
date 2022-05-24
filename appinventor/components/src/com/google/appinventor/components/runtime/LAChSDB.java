// -*- mode: java; c-basic-offset: 2; -*-
// Copyright 2017-2020 MIT, All rights reserved
// Released under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

package com.google.appinventor.components.runtime;

import static android.Manifest.permission.ACCESS_NETWORK_STATE;
import static android.Manifest.permission.INTERNET;

import android.Manifest;
import android.app.Activity;

import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;

import android.net.ConnectivityManager;
import android.net.NetworkInfo;

import android.os.Handler;

import android.util.Base64;
import android.util.Log;

import com.google.appinventor.components.annotations.DesignerComponent;
import com.google.appinventor.components.annotations.DesignerProperty;
import com.google.appinventor.components.annotations.PropertyCategory;
import com.google.appinventor.components.annotations.SimpleEvent;
import com.google.appinventor.components.annotations.SimpleFunction;
import com.google.appinventor.components.annotations.SimpleProperty;
import com.google.appinventor.components.annotations.UsesLibraries;
import com.google.appinventor.components.annotations.UsesPermissions;

import com.google.appinventor.components.common.ComponentCategory;
import com.google.appinventor.components.common.PropertyTypeConstants;
import com.google.appinventor.components.common.YaVersion;

import com.google.appinventor.components.runtime.errors.YailRuntimeError;

import com.google.appinventor.components.runtime.util.BulkPermissionRequest;
import com.google.appinventor.components.runtime.util.CloudDBJedisListener;
import com.google.appinventor.components.runtime.util.FileUtil;
import com.google.appinventor.components.runtime.util.JsonUtil;
import com.google.appinventor.components.runtime.util.YailList;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import org.json.JSONArray;
import org.json.JSONException;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.exceptions.JedisNoScriptException;

/**
 * The `CloudDB` component is a Non-visible component that allows you to store data on a Internet
 * connected database server (using Redis software). This allows the users of your App to share
 * data with each other. By default data will be stored in a server maintained by MIT, however you
 * can setup and run your own server. Set the {@link #RedisServer(String)} property and
 * {@link #RedisPort(int)} property to access your own server.
 *
 * @internaldoc
 * The component has methods to store a value under a tag and to
 * retrieve the value associated with the tag. It also possesses a listener to fire events
 * when stored values are changed. It also posseses a sync capability which helps CloudDB
 * to sync with data collected offline.
 *
 * @author manting@mit.edu (Natalie Lao)
 * @author joymitro1989@gmail.com (Joydeep Mitra)
 * @author jis@mit.edu (Jeffrey I. Schiller)
 */

@DesignerComponent(version = YaVersion.CLOUDDB_COMPONENT_VERSION,
    description = "Non-visible component allowing you to store data on a Internet " +
        "connected database server (using Redis software). This allows the users of " +
        "your App to share data with each other. " +
        "By default data will be stored in a server maintained by MIT, however you " +
        "can setup and run your own server. Set the \"RedisServer\" property and " +
        "\"RedisPort\" Property to access your own server.",
    designerHelpDescription = "Non-visible component that communicates with CloudDB " +
        "server to store and retrieve information.",
    category = ComponentCategory.STORAGE,
    nonVisible = true,
    iconName = "images/cloudDB.png")
@UsesPermissions({INTERNET, ACCESS_NETWORK_STATE})
@UsesLibraries(libraries = "jedis.jar")
public final class LAChSDB extends CloudDB implements Component,
 OnClearListener, OnDestroyListener {
  private static final boolean DEBUG = false;
  private static final String LOG_TAG = "CloudDB";
  private boolean importProject = false;
  private String projectID = "";
  private String token = "";
  private boolean isPublic = false;

  private volatile boolean dead = false; // On certain fatal errors we declare ourselves
                                         // "dead" which means an application restart
                                         // is required to get things going again.
                                         // For now, only an authentication error
                                         // sets this

  private static final String COMODO_ROOT =
    "-----BEGIN CERTIFICATE-----\n" +
    "MIIENjCCAx6gAwIBAgIBATANBgkqhkiG9w0BAQUFADBvMQswCQYDVQQGEwJTRTEU\n" +
    "MBIGA1UEChMLQWRkVHJ1c3QgQUIxJjAkBgNVBAsTHUFkZFRydXN0IEV4dGVybmFs\n" +
    "IFRUUCBOZXR3b3JrMSIwIAYDVQQDExlBZGRUcnVzdCBFeHRlcm5hbCBDQSBSb290\n" +
    "MB4XDTAwMDUzMDEwNDgzOFoXDTIwMDUzMDEwNDgzOFowbzELMAkGA1UEBhMCU0Ux\n" +
    "FDASBgNVBAoTC0FkZFRydXN0IEFCMSYwJAYDVQQLEx1BZGRUcnVzdCBFeHRlcm5h\n" +
    "bCBUVFAgTmV0d29yazEiMCAGA1UEAxMZQWRkVHJ1c3QgRXh0ZXJuYWwgQ0EgUm9v\n" +
    "dDCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBALf3GjPm8gAELTngTlvt\n" +
    "H7xsD821+iO2zt6bETOXpClMfZOfvUq8k+0DGuOPz+VtUFrWlymUWoCwSXrbLpX9\n" +
    "uMq/NzgtHj6RQa1wVsfwTz/oMp50ysiQVOnGXw94nZpAPA6sYapeFI+eh6FqUNzX\n" +
    "mk6vBbOmcZSccbNQYArHE504B4YCqOmoaSYYkKtMsE8jqzpPhNjfzp/haW+710LX\n" +
    "a0Tkx63ubUFfclpxCDezeWWkWaCUN/cALw3CknLa0Dhy2xSoRcRdKn23tNbE7qzN\n" +
    "E0S3ySvdQwAl+mG5aWpYIxG3pzOPVnVZ9c0p10a3CitlttNCbxWyuHv77+ldU9U0\n" +
    "WicCAwEAAaOB3DCB2TAdBgNVHQ4EFgQUrb2YejS0Jvf6xCZU7wO94CTLVBowCwYD\n" +
    "VR0PBAQDAgEGMA8GA1UdEwEB/wQFMAMBAf8wgZkGA1UdIwSBkTCBjoAUrb2YejS0\n" +
    "Jvf6xCZU7wO94CTLVBqhc6RxMG8xCzAJBgNVBAYTAlNFMRQwEgYDVQQKEwtBZGRU\n" +
    "cnVzdCBBQjEmMCQGA1UECxMdQWRkVHJ1c3QgRXh0ZXJuYWwgVFRQIE5ldHdvcmsx\n" +
    "IjAgBgNVBAMTGUFkZFRydXN0IEV4dGVybmFsIENBIFJvb3SCAQEwDQYJKoZIhvcN\n" +
    "AQEFBQADggEBALCb4IUlwtYj4g+WBpKdQZic2YR5gdkeWxQHIzZlj7DYd7usQWxH\n" +
    "YINRsPkyPef89iYTx4AWpb9a/IfPeHmJIZriTAcKhjW88t5RxNKWt9x+Tu5w/Rw5\n" +
    "6wwCURQtjr0W4MHfRnXnJK3s9EK0hZNwEGe6nQY1ShjTK3rMUUKhemPR5ruhxSvC\n" +
    "Nr4TDea9Y355e6cJDUCrat2PisP29owaQgVR1EX1n6diIWgVIEM8med8vSTYqZEX\n" +
    "c4g/VhsxOBi0cQ+azcgOno4uG+GMmIPLHzHxREzGBHNJdmAPx/i9F4BrLunMTA5a\n" +
    "mnkPIAou1Z5jJh5VkpTYghdae9C8x49OhgQ=\n" +
    "-----END CERTIFICATE-----\n";
  
  // We have to include this intermediate certificate because of bugs
  // in older versions of Android

  private static final String COMODO_USRTRUST =
    "-----BEGIN CERTIFICATE-----\n" +
    "MIIFdzCCBF+gAwIBAgIQE+oocFv07O0MNmMJgGFDNjANBgkqhkiG9w0BAQwFADBv\n" +
    "MQswCQYDVQQGEwJTRTEUMBIGA1UEChMLQWRkVHJ1c3QgQUIxJjAkBgNVBAsTHUFk\n" +
    "ZFRydXN0IEV4dGVybmFsIFRUUCBOZXR3b3JrMSIwIAYDVQQDExlBZGRUcnVzdCBF\n" +
    "eHRlcm5hbCBDQSBSb290MB4XDTAwMDUzMDEwNDgzOFoXDTIwMDUzMDEwNDgzOFow\n" +
    "gYgxCzAJBgNVBAYTAlVTMRMwEQYDVQQIEwpOZXcgSmVyc2V5MRQwEgYDVQQHEwtK\n" +
    "ZXJzZXkgQ2l0eTEeMBwGA1UEChMVVGhlIFVTRVJUUlVTVCBOZXR3b3JrMS4wLAYD\n" +
    "VQQDEyVVU0VSVHJ1c3QgUlNBIENlcnRpZmljYXRpb24gQXV0aG9yaXR5MIICIjAN\n" +
    "BgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEAgBJlFzYOw9sIs9CsVw127c0n00yt\n" +
    "UINh4qogTQktZAnczomfzD2p7PbPwdzx07HWezcoEStH2jnGvDoZtF+mvX2do2NC\n" +
    "tnbyqTsrkfjib9DsFiCQCT7i6HTJGLSR1GJk23+jBvGIGGqQIjy8/hPwhxR79uQf\n" +
    "jtTkUcYRZ0YIUcuGFFQ/vDP+fmyc/xadGL1RjjWmp2bIcmfbIWax1Jt4A8BQOujM\n" +
    "8Ny8nkz+rwWWNR9XWrf/zvk9tyy29lTdyOcSOk2uTIq3XJq0tyA9yn8iNK5+O2hm\n" +
    "AUTnAU5GU5szYPeUvlM3kHND8zLDU+/bqv50TmnHa4xgk97Exwzf4TKuzJM7UXiV\n" +
    "Z4vuPVb+DNBpDxsP8yUmazNt925H+nND5X4OpWaxKXwyhGNVicQNwZNUMBkTrNN9\n" +
    "N6frXTpsNVzbQdcS2qlJC9/YgIoJk2KOtWbPJYjNhLixP6Q5D9kCnusSTJV882sF\n" +
    "qV4Wg8y4Z+LoE53MW4LTTLPtW//e5XOsIzstAL81VXQJSdhJWBp/kjbmUZIO8yZ9\n" +
    "HE0XvMnsQybQv0FfQKlERPSZ51eHnlAfV1SoPv10Yy+xUGUJ5lhCLkMaTLTwJUdZ\n" +
    "+gQek9QmRkpQgbLevni3/GcV4clXhB4PY9bpYrrWX1Uu6lzGKAgEJTm4Diup8kyX\n" +
    "HAc/DVL17e8vgg8CAwEAAaOB9DCB8TAfBgNVHSMEGDAWgBStvZh6NLQm9/rEJlTv\n" +
    "A73gJMtUGjAdBgNVHQ4EFgQUU3m/WqorSs9UgOHYm8Cd8rIDZsswDgYDVR0PAQH/\n" +
    "BAQDAgGGMA8GA1UdEwEB/wQFMAMBAf8wEQYDVR0gBAowCDAGBgRVHSAAMEQGA1Ud\n" +
    "HwQ9MDswOaA3oDWGM2h0dHA6Ly9jcmwudXNlcnRydXN0LmNvbS9BZGRUcnVzdEV4\n" +
    "dGVybmFsQ0FSb290LmNybDA1BggrBgEFBQcBAQQpMCcwJQYIKwYBBQUHMAGGGWh0\n" +
    "dHA6Ly9vY3NwLnVzZXJ0cnVzdC5jb20wDQYJKoZIhvcNAQEMBQADggEBAJNl9jeD\n" +
    "lQ9ew4IcH9Z35zyKwKoJ8OkLJvHgwmp1ocd5yblSYMgpEg7wrQPWCcR23+WmgZWn\n" +
    "RtqCV6mVksW2jwMibDN3wXsyF24HzloUQToFJBv2FAY7qCUkDrvMKnXduXBBP3zQ\n" +
    "YzYhBx9G/2CkkeFnvN4ffhkUyWNnkepnB2u0j4vAbkN9w6GAbLIevFOFfdyQoaS8\n" +
    "Le9Gclc1Bb+7RrtubTeZtv8jkpHGbkD4jylW6l/VXxRTrPBPYer3IsynVgviuDQf\n" +
    "Jtl7GQVoP7o81DgGotPmjw7jtHFtQELFhLRAlSv0ZaBIefYdgWOWnU914Ph85I6p\n" +
    "0fKtirOMxyHNwu8=\n" +
    "-----END CERTIFICATE-----\n";

  // Digital Signature Trust Root X3 -- For Letsencrypt

  private static final String DST_ROOT_X3 =
    "-----BEGIN CERTIFICATE-----\n" +
    "MIIDSjCCAjKgAwIBAgIQRK+wgNajJ7qJMDmGLvhAazANBgkqhkiG9w0BAQUFADA/\n" +
    "MSQwIgYDVQQKExtEaWdpdGFsIFNpZ25hdHVyZSBUcnVzdCBDby4xFzAVBgNVBAMT\n" +
    "DkRTVCBSb290IENBIFgzMB4XDTAwMDkzMDIxMTIxOVoXDTIxMDkzMDE0MDExNVow\n" +
    "PzEkMCIGA1UEChMbRGlnaXRhbCBTaWduYXR1cmUgVHJ1c3QgQ28uMRcwFQYDVQQD\n" +
    "Ew5EU1QgUm9vdCBDQSBYMzCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEB\n" +
    "AN+v6ZdQCINXtMxiZfaQguzH0yxrMMpb7NnDfcdAwRgUi+DoM3ZJKuM/IUmTrE4O\n" +
    "rz5Iy2Xu/NMhD2XSKtkyj4zl93ewEnu1lcCJo6m67XMuegwGMoOifooUMM0RoOEq\n" +
    "OLl5CjH9UL2AZd+3UWODyOKIYepLYYHsUmu5ouJLGiifSKOeDNoJjj4XLh7dIN9b\n" +
    "xiqKqy69cK3FCxolkHRyxXtqqzTWMIn/5WgTe1QLyNau7Fqckh49ZLOMxt+/yUFw\n" +
    "7BZy1SbsOFU5Q9D8/RhcQPGX69Wam40dutolucbY38EVAjqr2m7xPi71XAicPNaD\n" +
    "aeQQmxkqtilX4+U9m5/wAl0CAwEAAaNCMEAwDwYDVR0TAQH/BAUwAwEB/zAOBgNV\n" +
    "HQ8BAf8EBAMCAQYwHQYDVR0OBBYEFMSnsaR7LHH62+FLkHX/xBVghYkQMA0GCSqG\n" +
    "SIb3DQEBBQUAA4IBAQCjGiybFwBcqR7uKGY3Or+Dxz9LwwmglSBd49lZRNI+DT69\n" +
    "ikugdB/OEIKcdBodfpga3csTS7MgROSR6cz8faXbauX+5v3gTt23ADq1cEmv8uXr\n" +
    "AvHRAosZy5Q6XkjEGB5YGV8eAlrwDPGxrancWYaLbumR9YbK+rlmM6pZW87ipxZz\n" +
    "R8srzJmwN0jP41ZL9c8PDHIyh8bwRLtTcm1D9SZImlJnt1ir/md2cXjbDaJWFBM5\n" +
    "JDGFoqgCWjBH4d1QB7wCCZAA62RjYJsWvIjJEubSfZGL+T0yjWW06XyxV3bqxbYo\n" +
    "Ob8VZRzI9neWagqNdwvYkQsEjgfbKbYK7p2CNTUQ\n" +
    "-----END CERTIFICATE-----\n";

  private String defaultRedisServer = null;
  private boolean useDefault = true;

  private Handler androidUIHandler;
  private final Activity activity;

  private Jedis INSTANCE = null;
  private volatile String redisServer = "DEFAULT";
  private volatile int redisPort;
  private volatile boolean useSSL = true;
  private volatile boolean shutdown = false; // Should this instance of CloudDB
                                             // stop?

  private SSLSocketFactory SslSockFactory = null; // Socket Factory for using
                                                  // SSL

  private volatile CloudDBJedisListener currentListener;
  private volatile boolean listenerRunning = false;

  // To avoid blocking the UI thread, we do most Jedis operations in the background.
  // Rather then spawning a new thread for each request, we use an ExcutorService with
  // a single background thread to perform all the Jedis work. Using a single thread
  // also means that we can share a single Jedis connection and not worry about thread
  // synchronization.

  private volatile ExecutorService background = Executors.newSingleThreadExecutor();

  // Store can be called frequenly and quickly in some situations. For example
  // using store inside of a Canvas Drag event (for realtime updating of a remote
  // canvas). Or in a handler for the Accelerometer (gasp!). To make storing as
  // effecient as possible, we have a queue of pending store requests and we
  // have a background task that drains this queue as fast as possible and
  // iterates over the queue until it is drained.
  private final List<storedValue> storeQueue = Collections.synchronizedList(new ArrayList());

  private ConnectivityManager cm;

  // private static class lachsStoredValue extends storedValue {
  //   private String tag;
  //   private JSONArray  valueList;
  //   private JSONArray purposes;

  //   lachsStoredValue(String tag, JSONArray valueList, JSONArray purposes) {
  //     super(tag, valueList);
  //     this.purposes = purposes;
  //   }

  //   public JSONArray getPurposes() {
  //     return purposes;
  //   }
  // }

  /**
   * Creates a new CloudDB component.
   * @param container the Form that this component is contained in.
   */
  public LAChSDB(ComponentContainer container) {
    // Use the CloudDB constructor
    super(container);
    // We use androidUIHandler when we set up operations that run asynchronously
    // in a separate thread, but which themselves want to cause actions
    // back in the UI thread.  They do this by posting those actions
    // to androidUIHandler.
    androidUIHandler = new Handler();
    this.activity = container.$context();
    //Defaults set in MockCloudDB.java in appengine/src/com/google/appinventor/client/editor/simple/components
    projectID = ""; // set in Designer
    token = ""; //set in Designer

    redisPort = 6381;
    cm = (ConnectivityManager) form.$context().getSystemService(android.content.Context.CONNECTIVITY_SERVICE);
  }

  private void stopListener() {
    // We do this on the UI thread to make sure it is complete
    // before we repoint the redis server (or port)
    if (DEBUG) {
      Log.d(LOG_TAG, "Listener stopping!");
    }
    if (currentListener != null) {
      currentListener.terminate();
      currentListener = null;
      listenerRunning = false;
    }
  }

  private synchronized void startListener() {
    // Retrieve new posts as they are added to the CloudDB.
    // Note: We use a real thread here rather then the background executor
    // because this thread will run effectively forever
    if (listenerRunning) {
      if (DEBUG) {
        Log.d(LOG_TAG, "StartListener while already running, no action taken");
      }
      return;
    }
    listenerRunning = true;
    if (DEBUG) {
      Log.d(LOG_TAG, "Listener starting!");
    }
    Thread t = new Thread() {
        public void run() {
          Jedis jedis = getJedis(true);
          if (jedis != null) {
            try {
              currentListener = new CloudDBJedisListener(LAChSDB.this);
              jedis.subscribe(currentListener, projectID);
            } catch (Exception e) {
              Log.e(LOG_TAG, "Error in listener thread", e);
              try {
                jedis.close();
              } catch (Exception ee) {
                // XXX
              }
              if (DEBUG) {
                Log.d(LOG_TAG, "Listener: connection to Redis failed, sleeping 3 seconds.");
              }
              try {
                Thread.sleep(3*1000);
              } catch (InterruptedException ee) {
              }
              if (DEBUG) {
                Log.d(LOG_TAG, "Woke up!");
              }
            }
          } else {
            if (DEBUG) {
              Log.d(LOG_TAG, "Listener: getJedis(true) returned null, retry in 3...");
            }
            try {
              Thread.sleep(3*1000);
            } catch (InterruptedException e) {
            }
            if (DEBUG) {
              Log.d(LOG_TAG, "Woke up! (2)");
            }
          }
          listenerRunning = false;
          if (!dead && !shutdown) {
            startListener();
          } else {
            if (DEBUG) {
              Log.d(LOG_TAG, "We are dead, listener not retrying.");
            }
          }
        }
      };
    t.start();
  }

  private static final String SET_SUB_SCRIPT =
    "local key = KEYS[1];" +
    "local value = ARGV[1];" +
    "local topublish = cjson.decode(ARGV[2]);" +
    "local project = ARGV[3];" +
    "local newtable = {};" +
    "table.insert(newtable, key);" +
    "table.insert(newtable, topublish);" +
    "redis.call(\"publish\", project, cjson.encode(newtable));" +
    "return redis.call('set', project .. \":\" .. key, value);";

  private static final String SET_SUB_SCRIPT_SHA1 = "765978e4c340012f50733280368a0ccc4a14dfb7";

  /**
   * Asks `CloudDB` to store the given `value`{:.variable.block} under the given
   * `tag`{:.text.block}.
   *
   * @param tag The tag to use
   * @param valueToStore The value to store. Can be any type of value (e.g.
   * number, text, boolean or list).
   */
  // TODO: modify for lachsdb
  @SimpleFunction(description = "Store a value at a tag.")
  public void StoreValue(final String tag, final Object valueToStore) {
    checkProjectIDNotBlank();
    final String value;
    NetworkInfo networkInfo = cm.getActiveNetworkInfo();
    boolean isConnected = networkInfo != null && networkInfo.isConnected();

    try {
      if (valueToStore != null) {
        String strval = valueToStore.toString();
        if (strval.startsWith("file:///") || strval.startsWith("/storage")) {
          value = JsonUtil.getJsonRepresentation(readFile(strval));
        } else {
          value = JsonUtil.getJsonRepresentation(valueToStore);
        }
      } else {
        value = "";
      }
    } catch(JSONException e) {
      throw new YailRuntimeError("Value failed to convert to JSON.", "JSON Creation Error.");
    }

    if (isConnected) {
      if (DEBUG) {
        Log.d(LOG_TAG,"Device is online...");
      }
      synchronized(storeQueue) {
        boolean kickit = false;
        if (storeQueue.size() == 0) { // Need to kick off the background task
          if (DEBUG) {
            Log.d(LOG_TAG, "storeQueue is zero length, kicking background");
          }
          kickit = true;
        } else {
          if (DEBUG) {
            Log.d(LOG_TAG, "storeQueue has " + storeQueue.size() + " entries");
          }
        }
        JSONArray valueList = new JSONArray();
        try {
          valueList.put(0, value);
        } catch (JSONException e) {
          throw new YailRuntimeError("JSON Error putting value.", "value is not convertable");
        }
        storedValue work  = new storedValue(tag, valueList);
        storeQueue.add(work);
        if (kickit) {
          background.submit(new Runnable() {
              public void run() {
                JSONArray pendingValueList = null;
                String pendingTag = null;
                String pendingValue = null;
                try {
                  storedValue work;
                  if (DEBUG) {
                    Log.d(LOG_TAG, "store background task running.");
                  }
                  while (true) {
                    synchronized(storeQueue) {
                      if (DEBUG) {
                        Log.d(LOG_TAG, "store: In background synchronized block");
                      }
                      int size = storeQueue.size();
                      if (size == 0) {
                        if (DEBUG) {
                          Log.d(LOG_TAG, "store background task exiting.");
                        }
                        work = null;
                      } else {
                        if (DEBUG) {
                          Log.d(LOG_TAG, "store: storeQueue.size() == " + size);
                        }
                        work = storeQueue.remove(0);
                        if (DEBUG) {
                          Log.d(LOG_TAG, "store: got work.");
                        }
                      }
                    }
                    if (DEBUG) {
                      Log.d(LOG_TAG, "store: left synchronized block");
                    }
                    if (work == null) {
                      try {
                        if (pendingTag != null) {
                          String jsonValueList = pendingValueList.toString();
                          if (DEBUG) {
                            Log.d(LOG_TAG, "Workqueue empty, sending pendingTag, valueListLength = " + pendingValueList.length());
                          }
                          jEval(SET_SUB_SCRIPT, SET_SUB_SCRIPT_SHA1, 1, pendingTag, pendingValue, jsonValueList, projectID);
                          UpdateDone(pendingTag, "StoreValue");
                        }
                      } catch (JedisException e) {
                        CloudDBError(e.getMessage());
                        flushJedis(true);
                      }
                      return;
                    }

                    String tag = work.getTag();
                    JSONArray valueList = work.getValueList();
                    if (tag == null || valueList == null) {
                      if (DEBUG) {
                        Log.d(LOG_TAG, "Either tag or value is null!");
                      }
                    } else {
                      if (DEBUG) {
                        Log.d(LOG_TAG, "Got Work: tag = " + tag + " value = " + valueList.get(0));
                      }
                    }
                    if (pendingTag == null) { // First time through this invocation
                      pendingTag = tag;
                      pendingValueList = valueList;
                      pendingValue = valueList.getString(0);
                    } else if (pendingTag.equals(tag)) { // work is for the same tag
                      pendingValue = valueList.getString(0);
                      pendingValueList.put(pendingValue);
                    } else {    // Work is for a different tag, send what we have
                      try {     // and add the new tag,incoming valuelist for the next round
                        String jsonValueList = pendingValueList.toString();
                        if (DEBUG) {
                          Log.d(LOG_TAG, "pendingTag changed sending pendingTag, valueListLength = " + pendingValueList.length());
                        }
                        jEval(SET_SUB_SCRIPT, SET_SUB_SCRIPT_SHA1, 1, pendingTag, pendingValue, jsonValueList, projectID);
                      } catch (JedisException e) {
                        CloudDBError(e.getMessage());
                        flushJedis(true);
                        storeQueue.clear(); // Flush pending changes, we are in
                        return;             // an error state
                      }
                      pendingTag = tag;
                      pendingValueList = valueList;
                      pendingValue = valueList.getString(0);
                    }
                  }
                } catch (Exception e) {
                Log.e(LOG_TAG, "Exception in store worker!", e);
                }
              }
            });
        }
      }
    } else {
      CloudDBError("Cannot store values off-line.");
    }
  }

  /**
   * `GetValue` asks `CloudDB` to get the value stored under the given tag.
   * It will pass the result to the {@link #GotValue(String, Object) event.
   * If there is no value stored under the tag, the
   * `valueIfTagNotThere`{:.variable.block} will be given.
   *
   * @param tag The tag whose value is to be retrieved.
   * @param valueIfTagNotThere The value to pass to the event if the tag does
   *                           not exist.
   */
  // TODO: modify for lachsdb
  @SimpleFunction(description = "Get the Value for a tag, doesn't return the " +
    "value but will cause a GotValue event to fire when the " +
    "value is looked up.")
  public void GetValue(final String tag, final Object valueIfTagNotThere) {
    if (DEBUG) {
      Log.d(LOG_TAG, "getting value ... for tag: " + tag);
    }
    checkProjectIDNotBlank();
    final AtomicReference<Object> value = new AtomicReference<Object>();
    Cursor cursor = null;
    SQLiteDatabase db = null;
    NetworkInfo networkInfo = cm.getActiveNetworkInfo();
    boolean isConnected = networkInfo != null && networkInfo.isConnected();

    if (isConnected) {
      // Set value to either the JSON from the CloudDB
      // or the JSON representation of valueIfTagNotThere
      background.submit(new Runnable() {
          public void run() {
            Jedis jedis = getJedis();
            try {
              if (DEBUG) {
                Log.d(LOG_TAG,"about to call jedis.get()");
              }
              String returnValue = jedis.get(projectID + ":" + tag);
              if (DEBUG) {
                Log.d(LOG_TAG, "finished call jedis.get()");
              }
              if (returnValue != null) {
                String val = JsonUtil.getJsonRepresentationIfValueFileName(form, returnValue);
                if(val != null) value.set(val);
                else value.set(returnValue);
              }
              else {
                if (DEBUG) {
                  Log.d(LAChSDB.LOG_TAG,"Value retrieved is null");
                }
                value.set(JsonUtil.getJsonRepresentation(valueIfTagNotThere));
              }
            } catch (JSONException e) {
              CloudDBError("JSON conversion error for " + tag);
              return;
            } catch (NullPointerException e) {
              CloudDBError("System Error getting tag " + tag);
              flushJedis(true);
              return;
            } catch (JedisException e) {
              Log.e(LOG_TAG, "Exception in GetValue", e);
              CloudDBError(e.getMessage());
              flushJedis(true);
              return;
            } catch (Exception e) {
              Log.e(LOG_TAG, "Exception in GetValue", e);
              CloudDBError(e.getMessage());
              flushJedis(true);
              return;
            }

            androidUIHandler.post(new Runnable() {
                public void run() {
                  // Signal an event to indicate that the value was
                  // received.  We post this to run in the Application's main
                  // UI thread.
                  GotValue(tag, value.get());
                }
              });
          }
        });
    } else {
      if (DEBUG) {
        Log.d(LOG_TAG, "GetValue(): We're offline");
      }
      CloudDBError("Cannot fetch variables while off-line.");
    }
  }

  private static final String POP_FIRST_SCRIPT =
      "local key = KEYS[1];" +
      "local project = ARGV[1];" +
      "local currentValue = redis.call('get', project .. \":\" .. key);" +
      "local decodedValue = cjson.decode(currentValue);" +
      "local subTable = {};" +
      "local subTable1 = {};" +
      "if (type(decodedValue) == 'table') then " +
      "  local removedValue = table.remove(decodedValue, 1);" +
      "  local newValue = cjson.encode(decodedValue);" +
      "  if (newValue == \"{}\") then " +
      "    newValue = \"[]\" " +
      "  end " +
      "  redis.call('set', project .. \":\" .. key, newValue);" +
      "  table.insert(subTable, key);" +
      "  table.insert(subTable1, newValue);" +
      "  table.insert(subTable, subTable1);" +
      "  redis.call(\"publish\", project, cjson.encode(subTable));" +
      "  return cjson.encode(removedValue);" +
      "else " +
      "  return error('You can only remove elements from a list');" +
      "end";

  private static final String POP_FIRST_SCRIPT_SHA1 = "68a7576e7dc283a8162d01e3e7c2d5c4ab3ff7a5";

  private static final String APPEND_SCRIPT =
      "local key = KEYS[1];" +
      "local toAppend = cjson.decode(ARGV[1]);" +
      "local project = ARGV[2];" +
      "local currentValue = redis.call('get', project .. \":\" .. key);" +
      "local newTable;" +
      "local subTable = {};" +
      "local subTable1 = {};" +
      "if (currentValue == false) then " +
      "  newTable = {};" +
      "else " +
      "  newTable = cjson.decode(currentValue);" +
      "  if not (type(newTable) == 'table') then " +
      "    return error('You can only append to a list');" +
      "  end " +
      "end " +
      "table.insert(newTable, toAppend);" +
      "local newValue = cjson.encode(newTable);" +
      "redis.call('set', project .. \":\" .. key, newValue);" +
      "table.insert(subTable1, newValue);" +
      "table.insert(subTable, key);" +
      "table.insert(subTable, subTable1);" +
      "redis.call(\"publish\", project, cjson.encode(subTable));" +
      "return newValue;";

  private static final String APPEND_SCRIPT_SHA1 = "d6cc0f65b29878589f00564d52c8654967e9bcf8";

  /**
   * Indicates that a {@link #GetValue(String, Object)} request has succeeded.
   *
   * @param value the value that was returned. Can be any type of value
   *              (e.g. number, text, boolean or list).
   */
  //TODO: modify for lachsdb
  @SimpleEvent
  public void GotValue(String tag, Object value) {
    if (DEBUG) {
      Log.d(LAChSDB.LOG_TAG, "GotValue: tag = " + tag + " value = " + (String) value);
    }
    checkProjectIDNotBlank();

    // We can get a null value is the Jedis connection failed in some way.
    // not sure what to do here, so we'll signal an error for now.
    if (value == null) {
      CloudDBError("Trouble getting " + tag + " from the server.");
      return;
    }

    try {
      if (DEBUG) {
        Log.d(LOG_TAG, "GotValue: Class of value = " + value.getClass().getName());
      }
      if(value != null && value instanceof String) {
        value = JsonUtil.getObjectFromJson((String) value, true);
      }
    } catch(JSONException e) {
      throw new YailRuntimeError("Value failed to convert from JSON.", "JSON Retrieval Error.");
    }

    // Invoke the application's "GotValue" event handler
    EventDispatcher.dispatchEvent(this, "GotValue", tag, value);
  }

  private void checkProjectIDNotBlank(){
    if (projectID.equals("")){
      throw new RuntimeException("CloudDB ProjectID property cannot be blank.");
    }
    if(token.equals("")){
      throw new RuntimeException("CloudDB Token property cannot be blank");
    }
  }

   /*
   * flushJedis -- Flush the singleton jedis connection. This is
   * used when we detect an error from jedis. It is possible that after
   * an error the jedis connection is in an invalid state (or closed) so
   * we want to make sure we get a new one the next time around!
   */

  private void flushJedis(boolean restartListener) {
    if (INSTANCE == null) {
      return;                   // Nothing to do
    }
    try {
      INSTANCE.close();         // Just in case we still have
                                // a connection
    } catch (Exception e) {
      // XXX
    }
    INSTANCE = null;
    // We are now going to kill the executor, as it may
    // have hung tasks. We do this on the UI thread as a
    // way to synchronize things.
    androidUIHandler.post(new Runnable() {
        public void run() {
          List <Runnable> tasks = background.shutdownNow();
          if (DEBUG) {
            Log.d(LOG_TAG, "Killing background executor, returned tasks = " + tasks);
          }
          background = Executors.newSingleThreadExecutor();
        }
      });

    stopListener();             // This is probably hosed to, so restart
    if (restartListener) {
      startListener();
    }
  }

  /**
   * Accepts a file name and returns a Yail List with two
   * elements. the first element is the file's extension (example:
   * jpg, gif, etc.). The second element is the base64 encoded
   * contents of the file. This function is suitable for reading
   * binary files such as sounds and images. The base64 contents can
   * then be stored with mechanisms oriented around text, such as
   * tinyDB, Fusion tables and Firebase.
   *
   * Written by Jeff Schiller (jis) for the BinFile Extension
   *
   * @param fileName
   * @returns YailList the list of the file extension and contents
   */
  private YailList readFile(String fileName) {
    try {
      String originalFileName = fileName;
      // Trim off file:// part if present
      if (fileName.startsWith("file://")) {
        fileName = fileName.substring(7);
      }
      if (!fileName.startsWith("/")) {
        throw new YailRuntimeError("Invalid fileName, was " + originalFileName, "ReadFrom");
      }
      String extension = getFileExtension(fileName);
      byte [] content = FileUtil.readFile(form, fileName);
      String encodedContent = Base64.encodeToString(content, Base64.DEFAULT);
      Object [] results = new Object[2];
      results[0] = "." + extension;
      results[1] = encodedContent;
      return YailList.makeList(results);
    } catch (FileNotFoundException e) {
      throw new YailRuntimeError(e.getMessage(), "Read");
    } catch (IOException e) {
      throw new YailRuntimeError(e.getMessage(), "Read");
    }
  }

  // Utility to get the file extension from a filename
  // Written by Jeff Schiller (jis) for the BinFile Extension
  private String getFileExtension(String fullName) {
    String fileName = new File(fullName).getName();
    int dotIndex = fileName.lastIndexOf(".");
    return dotIndex == -1 ? "" : fileName.substring(dotIndex + 1);
  }

  // We are synchronized because we are called simultaneously from two
  // different threads. Rather then do the work twice, the first one
  // does the work and the second one waits!
  private synchronized void ensureSslSockFactory() {
    if (SslSockFactory != null) {
      return;
    } else {
      try {
        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        ByteArrayInputStream caInput = new ByteArrayInputStream(COMODO_ROOT.getBytes("UTF-8"));
        Certificate ca = cf.generateCertificate(caInput);
        caInput.close();
        caInput = new ByteArrayInputStream(COMODO_USRTRUST.getBytes("UTF-8"));
        Certificate inter = cf.generateCertificate(caInput);
        caInput.close();
        caInput = new ByteArrayInputStream(DST_ROOT_X3.getBytes("UTF-8"));
        Certificate dstx3 = cf.generateCertificate(caInput);
        caInput.close();
        if (DEBUG) {
          Log.d(LOG_TAG, "comodo=" + ((X509Certificate) ca).getSubjectDN());
          Log.d(LOG_TAG, "inter=" + ((X509Certificate) inter).getSubjectDN());
          Log.d(LOG_TAG, "dstx3=" + ((X509Certificate) dstx3).getSubjectDN());
        }
        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        keyStore.load(null, null);
        // First add the system trusted certificates
        int count = 1;
        for (X509Certificate cert : getSystemCertificates()) {
          keyStore.setCertificateEntry("root" + count, cert);
          count += 1;
        }
        if (DEBUG) {
          Log.d(LOG_TAG, "Added " + (count -1) + " system certificates!");
        }
        // Now add our additions
        keyStore.setCertificateEntry("comodo", ca);
        keyStore.setCertificateEntry("inter", inter);
        keyStore.setCertificateEntry("dstx3", dstx3);
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(
          TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(keyStore);
        // // DEBUG
        // Log.d(LOG_TAG, "And now for something completely different...");
        // X509TrustManager tm = (X509TrustManager) tmf.getTrustManagers()[0];
        // for (X509Certificate cert : tm.getAcceptedIssuers()) {
        //   Log.d(LOG_TAG, cert.getSubjectX500Principal().getName());
        // }
        // // END DEBUG
        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(null, tmf.getTrustManagers(), null);
        SslSockFactory = ctx.getSocketFactory();
      } catch (Exception e) {
        Log.e(LOG_TAG, "Could not setup SSL Trust Store for CloudDB", e);
        throw new YailRuntimeError("Could Not setup SSL Trust Store for CloudDB: ", e.getMessage());
      }
    }
  }

  /*
   * Get the list of root CA's trusted by this device
   *
   */
  private X509Certificate[] getSystemCertificates() {
    try {
      TrustManagerFactory otmf = TrustManagerFactory.getInstance(
        TrustManagerFactory.getDefaultAlgorithm());
      otmf.init((KeyStore) null);
      X509TrustManager otm = (X509TrustManager) otmf.getTrustManagers()[0];
      return otm.getAcceptedIssuers();
    } catch (Exception e) {
      Log.e(LOG_TAG, "Getting System Certificates", e);
      return new X509Certificate[0];
    }
  }
}
