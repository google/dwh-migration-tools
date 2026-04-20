package com.google.edwmigration.dumper.build.licensereport

class OverridesUtil {

    static Map<String, Map<String, String>> projectOverrides() {
        return [
            "ch.qos.logback:logback-classic": [
                projectUrl: "https://github.com/qos-ch/logback",
                licenseUrl: "https://raw.githubusercontent.com/qos-ch/logback/master/LICENSE.txt",
            ],
            "ch.qos.logback:logback-core": [
                projectUrl: "https://github.com/qos-ch/logback",
                licenseUrl: "https://raw.githubusercontent.com/qos-ch/logback/master/LICENSE.txt",
            ],
            "com.amazon.redshift:redshift-jdbc42": [
                licenseUrl: "https://raw.githubusercontent.com/aws/amazon-redshift-jdbc-driver/master/LICENSE",
            ],
            "com.amazonaws:aws-java-sdk-core": [
                licenseUrl: "https://raw.githubusercontent.com/aws/aws-sdk-java/master/LICENSE.txt",
            ],
            "com.amazonaws:aws-java-sdk-cloudwatch": [
                licenseUrl: "https://raw.githubusercontent.com/aws/aws-sdk-java/master/LICENSE.txt",
            ],
            "com.amazonaws:aws-java-sdk-redshift": [
                licenseUrl: "https://raw.githubusercontent.com/aws/aws-sdk-java/master/LICENSE.txt",
            ],
            "com.amazonaws:jmespath-java": [
                licenseUrl: "https://raw.githubusercontent.com/aws/aws-sdk-java/master/LICENSE.txt",
            ],
            "com.fasterxml.jackson:jackson-bom": [
                projectUrl: "https://github.com/FasterXML/jackson-bom",
                licenseName: "Apache License, Version 2.0",
                licenseUrl: "https://raw.githubusercontent.com/FasterXML/jackson-bom/2.16/LICENSE",
            ],
            "com.fasterxml.jackson.datatype:jackson-datatype-guava": [
                licenseUrl: "https://github.com/FasterXML/jackson-datatypes-collections/raw/refs/heads/2.19/LICENSE",
            ],
            "com.fasterxml.jackson.core:jackson-annotations": [
                licenseUrl: "https://raw.githubusercontent.com/FasterXML/jackson-core/2.16/LICENSE",
            ],
            "com.fasterxml.jackson.core:jackson-core": [
                licenseUrl: "https://raw.githubusercontent.com/FasterXML/jackson-core/2.16/LICENSE",
            ],
            "com.fasterxml.jackson.core:jackson-databind": [
                licenseUrl: "https://raw.githubusercontent.com/FasterXML/jackson-dataformats-binary/2.16/LICENSE",
            ],
            "com.fasterxml.jackson.dataformat:jackson-dataformat-cbor": [
                licenseUrl: "https://raw.githubusercontent.com/FasterXML/jackson-dataformats-binary/2.16/LICENSE",
            ],
            "com.fasterxml.jackson.dataformat:jackson-dataformat-csv": [
                licenseUrl: "https://github.com/FasterXML/jackson-dataformats-text/blob/2.19/LICENSE",
            ],
            "com.fasterxml.jackson.dataformat:jackson-dataformat-yaml": [
                licenseUrl: "https://raw.githubusercontent.com/FasterXML/jackson-dataformats-text/2.16/LICENSE",
            ],
            "com.fasterxml.jackson.datatype:jackson-datatype-jsr310": [
                projectUrl: "https://github.com/FasterXML/jackson-modules-java8",
                licenseUrl: "https://raw.githubusercontent.com/FasterXML/jackson-modules-java8/2.16/LICENSE",
            ],
            "com.github.freva:ascii-table":[
                licenseUrl: "https://github.com/freva/ascii-table/blob/master/LICENSE"
            ],
            "com.github.stephenc.jcip:jcip-annotations": [
                projectUrl: "https://github.com/stephenc/jcip-annotations",
                licenseUrl: "https://raw.githubusercontent.com/stephenc/jcip-annotations/master/LICENSE.txt,"
            ],
            "com.google.android:annotations": [
                licenseUrl: "https://raw.githubusercontent.com/androidannotations/androidannotations/develop/LICENSE.txt",
            ],
            "com.google.api:api-common": [
                projectUrl: "https://github.com/googleapis/api-common-java",
                licenseName: "The 3-Clause BSD License",
                licenseUrl: "https://raw.githubusercontent.com/googleapis/api-common-java/main/LICENSE",
            ],
            "com.google.api:gax": [
                projectUrl: "https://github.com/googleapis/gax-java",
                licenseName: "The 3-Clause BSD License",
                licenseUrl: "https://raw.githubusercontent.com/googleapis/gax-java/main/LICENSE",
            ],
            "com.google.api:gax-grpc": [
                projectUrl: "https://github.com/googleapis/gax-java",
                licenseName: "The 3-Clause BSD License",
                licenseUrl: "https://raw.githubusercontent.com/googleapis/gax-java/main/LICENSE",
            ],
            "com.google.api:gax-httpjson": [
                projectUrl: "https://github.com/googleapis/gax-java",
                licenseName: "The 3-Clause BSD License",
                licenseUrl: "https://raw.githubusercontent.com/googleapis/gax-java/main/LICENSE",
            ],
            "com.google.api-client:google-api-client": [
                projectUrl: "https://github.com/googleapis/google-api-java-client",
                licenseUrl: "https://raw.githubusercontent.com/googleapis/google-api-java-client/main/LICENSE",
            ],
            "com.google.api.grpc:gapic-google-cloud-storage-v2": [
                projectUrl: "https://github.com/googleapis/googleapis",
                licenseUrl: "https://raw.githubusercontent.com/googleapis/googleapis/master/LICENSE",
            ],
            "com.google.api.grpc:grpc-google-cloud-bigquerystorage-v1": [
                licenseUrl: "https://raw.githubusercontent.com/google/flatbuffers/master/LICENSE",
            ],
            "com.google.api.grpc:grpc-google-cloud-bigquerystorage-v1beta1": [
                licenseUrl: "https://raw.githubusercontent.com/google/flatbuffers/master/LICENSE",
            ],
            "com.google.api.grpc:grpc-google-cloud-bigquerystorage-v1beta2": [
                licenseUrl: "https://raw.githubusercontent.com/google/flatbuffers/master/LICENSE",
            ],
            "com.google.api.grpc:grpc-google-cloud-storage-control-v2": [
                licenseUrl: "https://raw.githubusercontent.com/google/flatbuffers/master/LICENSE",
            ],
            "com.google.api.grpc:grpc-google-cloud-storage-v2": [
                projectUrl: "https://github.com/googleapis/java-storage",
                licenseUrl: "https://raw.githubusercontent.com/googleapis/java-storage/main/LICENSE",
            ],
            "com.google.api.grpc:grpc-google-common-protos": [
                licenseUrl: "https://raw.githubusercontent.com/googleapis/sdk-platform-java/main/LICENSE",
            ],
            "com.google.api.grpc:grpc-google-iam-v1": [
                licenseUrl: "https://raw.githubusercontent.com/googleapis/sdk-platform-java/main/LICENSE",
            ],
            "com.google.api.grpc:proto-google-cloud-bigquerydatatransfer-v1": [
                licenseUrl: "https://github.com/googleapis/java-bigquery-datatransfer/blob/main/LICENSE"
            ],
            "com.google.api.grpc:proto-google-cloud-bigquerystorage-v1": [
                projectUrl: "https://github.com/googleapis/googleapis",
                licenseUrl: "https://raw.githubusercontent.com/googleapis/googleapis/master/LICENSE",
            ],
            "com.google.api.grpc:proto-google-cloud-bigquerystorage-v1beta1": [
                projectUrl: "https://github.com/googleapis/googleapis",
                licenseUrl: "https://raw.githubusercontent.com/googleapis/googleapis/master/LICENSE",
            ],
            "com.google.api.grpc:proto-google-cloud-bigquerystorage-v1beta2": [
                projectUrl: "https://github.com/googleapis/googleapis",
                licenseUrl: "https://raw.githubusercontent.com/googleapis/googleapis/master/LICENSE",
            ],
            "com.google.api.grpc:proto-google-cloud-bigquerystorage-v1alpha": [
                projectUrl: "https://github.com/googleapis/googleapis",
                licenseUrl: "https://raw.githubusercontent.com/googleapis/googleapis/master/LICENSE",
            ],
            "com.google.api.grpc:proto-google-cloud-storage-control-v2": [
                licenseUrl: "https://raw.githubusercontent.com/googleapis/googleapis/master/LICENSE"
            ],
            "com.google.api.grpc:proto-google-cloud-kms-v1": [
                licenseUrl: "https://raw.githubusercontent.com/googleapis/google-cloud-java/main/LICENSE",
            ],
            "com.google.api.grpc:proto-google-cloud-logging-v2": [
                licenseUrl: "https://github.com/googleapis/java-logging/blob/main/LICENSE"
            ],
            "com.google.api.grpc:proto-google-cloud-monitoring-v3": [
                licenseUrl: "https://raw.githubusercontent.com/googleapis/google-cloud-java/main/LICENSE",
            ],
            "com.google.api.grpc:proto-google-cloud-storage-v2": [
                projectUrl: "https://github.com/googleapis/googleapis",
                licenseUrl: "https://raw.githubusercontent.com/googleapis/googleapis/master/LICENSE",
            ],
            "com.google.api.grpc:proto-google-common-protos": [
                licenseUrl: "https://raw.githubusercontent.com/googleapis/sdk-platform-java/main/LICENSE",
            ],
            "com.google.api.grpc:proto-google-iam-v1": [
                licenseUrl: "https://raw.githubusercontent.com/googleapis/sdk-platform-java/main/LICENSE",
            ],
            "com.google.apis:google-api-services-bigquery": [
                projectUrl: "https://github.com/googleapis/java-bigquery",
                licenseUrl: "https://raw.githubusercontent.com/googleapis/java-bigquery/main/LICENSE",
            ],
            "com.google.apis:google-api-services-storage": [
                projectUrl: "https://github.com/googleapis/google-api-java-client-services",
                licenseUrl: "https://raw.githubusercontent.com/googleapis/google-api-java-client-services/main/LICENSE",
            ],
            "com.google.auth:google-auth-library-credentials": [
                projectUrl: "https://github.com/googleapis/google-auth-library-java",
                licenseUrl: "https://raw.githubusercontent.com/googleapis/google-auth-library-java/main/LICENSE",
            ],
            "com.google.auth:google-auth-library-oauth2-http": [
                projectUrl: "https://github.com/googleapis/google-auth-library-java",
                licenseUrl: "https://raw.githubusercontent.com/googleapis/google-auth-library-java/main/LICENSE",
            ],
            "com.google.auto.value:auto-value": [
                licenseUrl: "https://raw.githubusercontent.com/google/auto/main/LICENSE",
            ],
            "com.google.auto.value:auto-value-annotations": [
                licenseUrl: "https://raw.githubusercontent.com/google/auto/main/LICENSE",
            ],
            "com.google.cloud:google-cloud-bigquery": [
                licenseUrl: "https://raw.githubusercontent.com/googleapis/java-bigquery/main/LICENSE",
            ],
            "com.google.cloud:google-cloud-bigquerystorage": [
                licenseUrl: "https://raw.githubusercontent.com/googleapis/java-bigquerystorage/main/LICENSE",
            ],
            "com.google.cloud:google-cloud-bigquerydatatransfer": [
                    licenseUrl: "https://github.com/googleapis/python-bigquery-datatransfer/blob/main/LICENSE"
            ],
            "com.google.cloud:google-cloud-core": [
                projectUrl: "https://github.com/googleapis/java-core",
                licenseUrl: "https://raw.githubusercontent.com/googleapis/java-core/main/LICENSE",
            ],
            "com.google.cloud:google-cloud-core-grpc": [
                projectUrl: "https://github.com/googleapis/java-core",
                licenseUrl: "https://raw.githubusercontent.com/googleapis/java-core/main/LICENSE",
            ],
            "com.google.cloud:google-cloud-core-http": [
                projectUrl: "https://github.com/googleapis/java-core",
                licenseUrl: "https://raw.githubusercontent.com/googleapis/java-core/main/LICENSE",
            ],
            "com.google.cloud:google-cloud-kms": [
                licenseUrl: "https://raw.githubusercontent.com/googleapis/google-cloud-java/main/LICENSE",
            ],
            "com.google.cloud:google-cloud-logging": [
                licenseUrl: "https://github.com/googleapis/java-logging/blob/main/LICENSE"
            ],
            "com.google.cloud:google-cloud-monitoring": [
                licenseUrl: "https://raw.githubusercontent.com/googleapis/java-core/main/LICENSE",
            ],
            "com.google.cloud:google-cloud-nio": [
                licenseUrl: "https://raw.githubusercontent.com/googleapis/java-storage-nio/main/LICENSE",
            ],
            "com.google.cloud:google-cloud-storage": [
                licenseUrl: "https://raw.githubusercontent.com/googleapis/java-storage/main/LICENSE",
            ],
            "com.google.cloud:google-cloud-storage-control": [
                    licenseUrl: "https://raw.githubusercontent.com/googleapis/googleapis/master/LICENSE"
            ],
            "com.google.code.findbugs:jsr305": [
                projectUrl: "http://code.google.com/p/jsr-305/",
                licenseName: "The 3-Clause BSD License",
                licenseUrl: "https://raw.githubusercontent.com/findbugsproject/findbugs/master/findbugs/licenses/LICENSE-jsr305.txt",
            ],
            "com.google.code.gson:gson": [
                projectUrl: "https://github.com/google/gson",
                licenseUrl: "https://raw.githubusercontent.com/google/gson/main/LICENSE",
            ],
            "com.google.errorprone:error_prone_annotations": [
                projectUrl: "https://github.com/google/error-prone",
                licenseUrl: "https://raw.githubusercontent.com/google/error-prone/master/COPYING",
            ],
            "com.google.flatbuffers:flatbuffers-java": [
                licenseUrl: "https://raw.githubusercontent.com/google/flatbuffers/master/LICENSE",
            ],
            "com.google.guava:guava": [
                licenseUrl: "https://raw.githubusercontent.com/google/guava/master/LICENSE",
            ],
            "com.google.guava:failureaccess": [
                projectUrl: "https://github.com/google/guava",
                licenseUrl: "https://raw.githubusercontent.com/google/guava/master/LICENSE",
            ],
            "com.google.guava:guava-parent": [
                projectUrl: "https://github.com/google/guava",
                licenseName: "Apache License, Version 2.0",
                licenseUrl: "https://raw.githubusercontent.com/google/guava/master/LICENSE",
            ],
            "com.google.guava:listenablefuture": [
                projectUrl: "https://github.com/google/guava",
                licenseUrl: "https://raw.githubusercontent.com/google/guava/master/LICENSE",
            ],
            "com.google.http-client:google-http-client": [
                projectUrl: "https://github.com/googleapis/google-http-java-client",
                licenseUrl: "https://raw.githubusercontent.com/googleapis/google-http-java-client/main/LICENSE",
            ],
            "com.google.http-client:google-http-client-apache-v2": [
                projectUrl: "https://github.com/googleapis/google-http-java-client",
                licenseUrl: "https://raw.githubusercontent.com/googleapis/google-http-java-client/main/LICENSE",
            ],
            "com.google.http-client:google-http-client-appengine": [
                projectUrl: "https://github.com/googleapis/google-http-java-client",
                licenseUrl: "https://raw.githubusercontent.com/googleapis/google-http-java-client/main/LICENSE",
            ],
            "com.google.http-client:google-http-client-gson": [
                projectUrl: "https://github.com/googleapis/google-http-java-client",
                licenseUrl: "https://raw.githubusercontent.com/googleapis/google-http-java-client/main/LICENSE",
            ],
            "com.google.http-client:google-http-client-jackson2": [
                projectUrl: "https://github.com/googleapis/google-http-java-client",
                licenseUrl: "https://raw.githubusercontent.com/googleapis/google-http-java-client/main/LICENSE",
            ],
            "com.google.j2objc:j2objc-annotations": [
                licenseUrl: "https://raw.githubusercontent.com/google/j2objc/master/LICENSE",
            ],
            "com.google.oauth-client:google-oauth-client": [
                projectUrl: "https://github.com/googleapis/google-oauth-java-client",
                licenseUrl: "https://raw.githubusercontent.com/googleapis/google-oauth-java-client/main/LICENSE",
            ],
            "com.google.cloud:libraries-bom": [
                licenseUrl: "https://raw.githubusercontent.com/googleapis/java-cloud-bom/refs/heads/main/LICENSE",
            ],
            "com.google.cloud.opentelemetry:detector-resources-support": [
                licenseUrl: "https://raw.githubusercontent.com/GoogleCloudPlatform/opentelemetry-operations-java/refs/heads/main/LICENSE",
            ],
            "com.google.cloud.opentelemetry:exporter-metrics": [
                licenseUrl: "https://raw.githubusercontent.com/GoogleCloudPlatform/opentelemetry-operations-java/refs/heads/main/LICENSE",
            ],
            "com.google.cloud.opentelemetry:shared-resourcemapping": [
                licenseUrl: "https://raw.githubusercontent.com/GoogleCloudPlatform/opentelemetry-operations-java/refs/heads/main/LICENSE",
            ],
            "com.google.protobuf:protobuf-java": [
                projectUrl: "https://github.com/protocolbuffers/protobuf",
                licenseUrl: "https://raw.githubusercontent.com/protocolbuffers/protobuf/main/LICENSE",
            ],
            "com.google.protobuf:protobuf-java-util": [
                projectUrl: "https://github.com/protocolbuffers/protobuf",
                licenseUrl: "https://raw.githubusercontent.com/protocolbuffers/protobuf/main/LICENSE",
            ],
            "com.google.re2j:re2j": [
                licenseUrl: "https://raw.githubusercontent.com/google/re2j/master/LICENSE",
            ],
            "com.swrve:rate-limited-logger": [
                licenseUrl: "https://raw.githubusercontent.com/Swrve/rate-limited-logger/master/LICENSE",
            ],
            "com.zaxxer:HikariCP": [
                licenseUrl: "https://raw.githubusercontent.com/brettwooldridge/HikariCP/dev/LICENSE",
            ],
            "commons-codec:commons-codec": [
                licenseUrl: "https://raw.githubusercontent.com/apache/commons-codec/master/LICENSE.txt",
            ],
            "commons-io:commons-io": [
                licenseUrl: "https://raw.githubusercontent.com/apache/commons-io/master/LICENSE.txt",
            ],
            "dev.cel:cel": [
                    licenseUrl: "https://github.com/google/cel-java/blob/main/LICENSE"
            ],
            "org.apache.hadoop:hadoop-common": [
                projectUrl: "https://github.com/apache/hadoop",
                licenseUrl: "https://raw.githubusercontent.com/apache/hadoop/trunk/LICENSE.txt",
            ],
            "org.apache.hadoop:hadoop-hdfs-client": [
                projectUrl: "https://github.com/apache/hadoop",
                licenseUrl: "https://raw.githubusercontent.com/apache/hadoop/trunk/LICENSE.txt",
            ],
            "io.grpc:grpc-alts": [
                licenseUrl: "https://raw.githubusercontent.com/grpc/grpc-java/master/LICENSE",
            ],
            "io.grpc:grpc-api": [
                licenseUrl: "https://raw.githubusercontent.com/grpc/grpc-java/master/LICENSE",
            ],
            "io.grpc:grpc-auth": [
                licenseUrl: "https://raw.githubusercontent.com/grpc/grpc-java/master/LICENSE",
            ],
            "io.grpc:grpc-context": [
                licenseUrl: "https://raw.githubusercontent.com/grpc/grpc-java/master/LICENSE",
            ],
            "io.grpc:grpc-core": [
                licenseUrl: "https://raw.githubusercontent.com/grpc/grpc-java/master/LICENSE",
            ],
            "io.grpc:grpc-googleapis": [
                licenseUrl: "https://raw.githubusercontent.com/grpc/grpc-java/master/LICENSE",
            ],
            "io.grpc:grpc-grpclb": [
                licenseUrl: "https://raw.githubusercontent.com/grpc/grpc-java/master/LICENSE",
            ],
            "io.grpc:grpc-inprocess": [
                licenseUrl: "https://raw.githubusercontent.com/grpc/grpc-java/master/LICENSE",
            ],
            "io.grpc:grpc-netty-shaded": [
                licenseUrl: "https://raw.githubusercontent.com/grpc/grpc-java/master/LICENSE",
            ],
            "io.grpc:grpc-opentelemetry": [
                licenseUrl: "https://raw.githubusercontent.com/grpc/grpc-java/master/LICENSE",
            ],
            "io.grpc:grpc-protobuf": [
                licenseUrl: "https://raw.githubusercontent.com/grpc/grpc-java/master/LICENSE",
            ],
            "io.grpc:grpc-protobuf-lite": [
                licenseUrl: "https://raw.githubusercontent.com/grpc/grpc-java/master/LICENSE",
            ],
            "io.grpc:grpc-rls": [
                licenseUrl: "https://raw.githubusercontent.com/grpc/grpc-java/master/LICENSE",
            ],
            "io.grpc:grpc-services": [
                licenseUrl: "https://raw.githubusercontent.com/grpc/grpc-java/master/LICENSE",
            ],
            "io.grpc:grpc-stub": [
                licenseUrl: "https://raw.githubusercontent.com/grpc/grpc-java/master/LICENSE",
            ],
            "io.grpc:grpc-util": [
                licenseUrl: "https://raw.githubusercontent.com/grpc/grpc-java/master/LICENSE",
            ],
            "io.grpc:grpc-xds": [
                licenseUrl: "https://raw.githubusercontent.com/grpc/grpc-java/master/LICENSE",
            ],
            "io.netty:netty-buffer": [
                projectUrl: "https://github.com/netty/netty",
                licenseUrl: "https://raw.githubusercontent.com/netty/netty/4.1/LICENSE.txt",
            ],
            "io.netty:netty-common": [
                projectUrl: "https://github.com/netty/netty",
                licenseUrl: "https://raw.githubusercontent.com/netty/netty/4.1/LICENSE.txt",
            ],
            "io.opencensus:opencensus-api": [
                licenseUrl: "https://raw.githubusercontent.com/census-instrumentation/opencensus-java/master/LICENSE",
            ],
            "io.opencensus:opencensus-contrib-http-util": [
                licenseUrl: "https://raw.githubusercontent.com/census-instrumentation/opencensus-java/master/LICENSE",
            ],
            "io.opencensus:opencensus-proto": [
                licenseUrl: "https://raw.githubusercontent.com/census-instrumentation/opencensus-java/master/LICENSE",
            ],
            "io.opentelemetry:opentelemetry-api": [
                licenseUrl: "https://raw.githubusercontent.com/open-telemetry/opentelemetry-java/refs/heads/main/LICENSE",
            ],
            "io.opentelemetry:opentelemetry-api-incubator": [
                licenseUrl: "https://raw.githubusercontent.com/open-telemetry/opentelemetry-java/refs/heads/main/LICENSE",
            ],
            "io.opentelemetry:opentelemetry-bom": [
                licenseUrl: "https://raw.githubusercontent.com/open-telemetry/opentelemetry-java/refs/heads/main/LICENSE",
            ],
            "io.opentelemetry:opentelemetry-context": [
                licenseUrl: "https://raw.githubusercontent.com/open-telemetry/opentelemetry-java/refs/heads/main/LICENSE",
            ],
            "io.opentelemetry:opentelemetry-sdk": [
                licenseUrl: "https://raw.githubusercontent.com/open-telemetry/opentelemetry-java/refs/heads/main/LICENSE",
            ],
            "io.opentelemetry:opentelemetry-sdk-common": [
                licenseUrl: "https://raw.githubusercontent.com/open-telemetry/opentelemetry-java/refs/heads/main/LICENSE",
            ],
            "io.opentelemetry:opentelemetry-sdk-extension-autoconfigure-spi": [
                licenseUrl: "https://raw.githubusercontent.com/open-telemetry/opentelemetry-java/refs/heads/main/LICENSE",
            ],
            "io.opentelemetry:opentelemetry-sdk-logs": [
                licenseUrl: "https://raw.githubusercontent.com/open-telemetry/opentelemetry-java/refs/heads/main/LICENSE",
            ],
            "io.opentelemetry:opentelemetry-sdk-metrics": [
                licenseUrl: "https://raw.githubusercontent.com/open-telemetry/opentelemetry-java/refs/heads/main/LICENSE",
            ],
            "io.opentelemetry:opentelemetry-sdk-trace": [
                licenseUrl: "https://raw.githubusercontent.com/open-telemetry/opentelemetry-java/refs/heads/main/LICENSE",
            ],
            "io.opentelemetry.contrib:opentelemetry-gcp-resources": [
                licenseUrl: "https://raw.githubusercontent.com/open-telemetry/opentelemetry-java/refs/heads/main/LICENSE",
            ],
            "io.opentelemetry.semconv:opentelemetry-semconv": [
                licenseUrl: "https://raw.githubusercontent.com/open-telemetry/opentelemetry-java/refs/heads/main/LICENSE",
            ],
            "io.perfmark:perfmark-api": [
                licenseUrl: "https://raw.githubusercontent.com/perfmark/perfmark/master/LICENSE",
            ],
            "javax.inject:javax.inject": [
                licenseUrl: "https://github.com/javax-inject/javax-inject#license",
            ],
            "joda-time:joda-time": [
                licenseUrl: "https://raw.githubusercontent.com/JodaOrg/joda-time/main/LICENSE.txt",
            ],
            "net.sf.jopt-simple:jopt-simple": [
                licenseUrl: "https://raw.githubusercontent.com/jopt-simple/jopt-simple/master/LICENSE.txt",
            ],
            "net.snowflake:snowflake-jdbc": [
                licenseUrl: "https://raw.githubusercontent.com/snowflakedb/snowflake-jdbc/master/LICENSE.txt",
            ],
            "org.anarres.jdiagnostics:jdiagnostics": [
                licenseUrl: "https://raw.githubusercontent.com/shevek/jdiagnostics/master/LICENSE",
            ],
            "org.antlr:antlr4-runtime": [
                licenseUrl: "https://github.com/antlr/antlr4/blob/dev/LICENSE.txt"
            ],
            "org.apache.arrow:arrow-format": [
                projectUrl: "https://github.com/apache/arrow",
                licenseUrl: "https://raw.githubusercontent.com/apache/arrow/main/LICENSE.txt",
            ],
            "org.apache.arrow:arrow-memory-core": [
                projectUrl: "https://github.com/apache/arrow",
                licenseUrl: "https://raw.githubusercontent.com/apache/arrow/main/LICENSE.txt",
            ],
            "org.apache.arrow:arrow-memory-netty": [
                projectUrl: "https://github.com/apache/arrow",
                licenseUrl: "https://raw.githubusercontent.com/apache/arrow/main/LICENSE.txt",
            ],
            "org.apache.arrow:arrow-vector": [
                projectUrl: "https://github.com/apache/arrow",
                licenseUrl: "https://raw.githubusercontent.com/apache/arrow/main/LICENSE.txt",
            ],
            "org.apache.commons:commons-csv": [
                licenseUrl: "https://raw.githubusercontent.com/apache/commons-csv/master/LICENSE.txt",
            ],
            "org.apache.commons:commons-lang3": [
                licenseUrl: "https://raw.githubusercontent.com/apache/commons-lang/master/LICENSE.txt",
            ],
            "org.apache.httpcomponents:httpclient": [
                licenseUrl: "https://raw.githubusercontent.com/apache/httpcomponents-client/master/LICENSE.txt",
            ],
            "org.apache.httpcomponents:httpcore": [
                licenseUrl: "https://raw.githubusercontent.com/apache/httpcomponents-core/master/LICENSE.txt",
            ],
            "org.apache.httpcomponents.client5:httpclient5": [
                projectUrl: "https://hc.apache.org/httpcomponents-client-5.2.x/",
                licenseUrl: "https://raw.githubusercontent.com/apache/httpcomponents-client/master/LICENSE.txt",
            ],
            "org.apache.httpcomponents.core5:httpcore5": [
                projectUrl: "https://hc.apache.org/httpcomponents-core-5.2.x/",
                licenseUrl: "https://raw.githubusercontent.com/apache/httpcomponents-core/master/LICENSE.txt",
            ],
            "org.apache.httpcomponents.core5:httpcore5-h2": [
                projectUrl: "https://hc.apache.org/httpcomponents-core-5.2.x/",
                licenseUrl: "https://raw.githubusercontent.com/apache/httpcomponents-core/master/LICENSE.txt",
            ],
            "org.apache.thrift:libthrift" : [
                licenseUrl: "https://raw.githubusercontent.com/apache/thrift/master/LICENSE",
            ],
            "org.checkerframework:checker-compat-qual": [
                licenseUrl: "https://raw.githubusercontent.com/typetools/checker-framework/master/LICENSE.txt",
            ],
            "org.checkerframework:checker-qual": [
                licenseUrl: "https://raw.githubusercontent.com/typetools/checker-framework/master/checker-qual/LICENSE.txt",
            ],
            "org.codehaus.mojo:animal-sniffer-annotations": [
                projectUrl: "https://www.mojohaus.org/animal-sniffer/animal-sniffer-maven-plugin/",
                licenseName: "MIT License",
                licenseUrl: "https://raw.githubusercontent.com/mojohaus/animal-sniffer/master/LICENSE",
            ],
            "org.conscrypt:conscrypt-openjdk-uber": [
                licenseUrl: "https://raw.githubusercontent.com/google/conscrypt/master/LICENSE",
            ],
            "org.jspecify:jspecify": [
                licenseUrl: "https://github.com/jspecify/jspecify/blob/main/LICENSE"
            ],
            "org.postgresql:postgresql": [
                licenseUrl: "https://raw.githubusercontent.com/pgjdbc/pgjdbc/master/LICENSE",
            ],
            "org.slf4j:jcl-over-slf4j": [
                licenseUrl: "https://raw.githubusercontent.com/qos-ch/slf4j/master/LICENSE.txt",
            ],
            "org.slf4j:slf4j-api": [
                licenseUrl: "https://raw.githubusercontent.com/qos-ch/slf4j/master/LICENSE.txt",
            ],
            "org.springframework:spring-beans": [
                licenseUrl: "https://raw.githubusercontent.com/spring-projects/spring-framework/main/LICENSE.txt",
            ],
            "org.springframework:spring-core": [
                licenseUrl: "https://raw.githubusercontent.com/spring-projects/spring-framework/main/LICENSE.txt",
            ],
            "org.springframework:spring-jdbc": [
                licenseUrl: "https://raw.githubusercontent.com/spring-projects/spring-framework/main/LICENSE.txt",
            ],
            "org.springframework:spring-tx": [
                licenseUrl: "https://raw.githubusercontent.com/spring-projects/spring-framework/main/LICENSE.txt",
            ],
            "org.threeten:threeten-extra": [
                licenseUrl: "https://raw.githubusercontent.com/ThreeTen/threeten-extra/main/LICENSE.txt",
            ],
            "org.threeten:threetenbp": [
                licenseUrl: "https://raw.githubusercontent.com/ThreeTen/threeten-extra/main/LICENSE.txt",
            ],
            "org.yaml:snakeyaml": [
                licenseUrl: "https://raw.githubusercontent.com/snakeyaml/snakeyaml/master/LICENSE.txt",
            ],
            "software.amazon.ion:ion-java": [
                licenseUrl: "https://raw.githubusercontent.com/amazon-ion/ion-java/master/LICENSE",
            ],
            "org.xerial.snappy:snappy-java": [
                licenseUrl: "https://raw.githubusercontent.com/xerial/snappy-java/master/LICENSE",
            ],
            "org.springframework:spring-beans": [
                licenseUrl: "https://raw.githubusercontent.com/spring-projects/spring-framework/main/LICENSE.txt",
            ],
            "org.springframework:spring-core": [
                licenseUrl: "https://raw.githubusercontent.com/spring-projects/spring-framework/main/LICENSE.txt",
            ],
            "org.springframework:spring-jdbc": [
                licenseUrl: "https://raw.githubusercontent.com/spring-projects/spring-framework/main/LICENSE.txt",
            ],
            "org.springframework:spring-tx": [
                licenseUrl: "https://raw.githubusercontent.com/spring-projects/spring-framework/main/LICENSE.txt",
            ],
            "org.slf4j:slf4j-reload4j": [
                projectUrl: "https://github.com/qos-ch/reload4j",
                licenseUrl: "https://raw.githubusercontent.com/qos-ch/reload4j/master/LICENSE",
            ],
            "ch.qos.reload4j:reload4j": [
                projectUrl: "https://github.com/qos-ch/reload4j",
                licenseUrl: "https://raw.githubusercontent.com/qos-ch/reload4j/master/LICENSE",
            ],
            "com.fasterxml.woodstox:woodstox-core": [
                licenseUrl: "https://raw.githubusercontent.com/FasterXML/woodstox/master/LICENSE",
            ],
            "com.github.pjfanning:jersey-json": [
                licenseUrl: "https://raw.githubusercontent.com/pjfanning/jersey-1.x/master/license.html",
            ],
            "com.nimbusds:nimbus-jose-jwt": [
                licenseUrl: "https://bitbucket.org/connect2id/nimbus-jose-jwt/raw/81efe49024e0a491a718160d1bb3cedd8a3be295/LICENSE.txt",
            ],
            "org.jline:jline": [
                projectUrl: "https://github.com/jline/jline3",
                licenseUrl: "https://raw.githubusercontent.com/jline/jline3/master/LICENSE.txt",
            ],
            "org.eclipse.jetty:jetty-http": [
                projectUrl: "https://github.com/jetty/jetty.project",
                licenseUrl: "https://raw.githubusercontent.com/jetty/jetty.project/jetty-12.0.x/LICENSE",
            ],
            "org.eclipse.jetty:jetty-io": [
                projectUrl: "https://github.com/jetty/jetty.project",
                licenseUrl: "https://raw.githubusercontent.com/jetty/jetty.project/jetty-12.0.x/LICENSE",
            ],
            "org.eclipse.jetty:jetty-security": [
                projectUrl: "https://github.com/jetty/jetty.project",
                licenseUrl: "https://raw.githubusercontent.com/jetty/jetty.project/jetty-12.0.x/LICENSE",
            ],
            "org.eclipse.jetty:jetty-server": [
                projectUrl: "https://github.com/jetty/jetty.project",
                licenseUrl: "https://raw.githubusercontent.com/jetty/jetty.project/jetty-12.0.x/LICENSE",
            ],
            "org.eclipse.jetty:jetty-servlet": [
                projectUrl: "https://github.com/jetty/jetty.project",
                licenseUrl: "https://raw.githubusercontent.com/jetty/jetty.project/jetty-12.0.x/LICENSE",
            ],
            "org.eclipse.jetty:jetty-util": [
                projectUrl: "https://github.com/jetty/jetty.project",
                licenseUrl: "https://raw.githubusercontent.com/jetty/jetty.project/jetty-12.0.x/LICENSE",
            ],
            "org.eclipse.jetty:jetty-util-ajax": [
                projectUrl: "https://github.com/jetty/jetty.project",
                licenseUrl: "https://raw.githubusercontent.com/jetty/jetty.project/jetty-12.0.x/LICENSE",
            ],
            "org.eclipse.jetty:jetty-webapp": [
                projectUrl: "https://github.com/jetty/jetty.project",
                licenseUrl: "https://raw.githubusercontent.com/jetty/jetty.project/jetty-12.0.x/LICENSE",
            ],
            "org.eclipse.jetty:jetty-xml": [
                projectUrl: "https://github.com/jetty/jetty.project",
                licenseUrl: "https://raw.githubusercontent.com/jetty/jetty.project/jetty-12.0.x/LICENSE",
            ],
            "org.bouncycastle:bcprov-jdk15on": [
                projectUrl: "https://github.com/bcgit/bc-java",
                licenseUrl: "https://raw.githubusercontent.com/bcgit/bc-java/main/LICENSE.html",
            ],
            "org.codehaus.jettison:jettison": [
                licenseUrl: "https://raw.githubusercontent.com/jettison-json/jettison/master/LICENSE",
            ],
            "org.codehaus.woodstox:stax2-api": [
                licenseUrl: "https://raw.githubusercontent.com/FasterXML/stax2-api/master/LICENSE",
            ],
            "org.apache.yetus:audience-annotations": [
                projectUrl: "https://github.com/apache/yetus",
                licenseUrl: "https://raw.githubusercontent.com/apache/yetus/main/LICENSE",
            ],
            "org.apache.zookeeper:zookeeper": [
                projectUrl: "https://github.com/apache/zookeeper",
                licenseUrl: "https://raw.githubusercontent.com/apache/zookeeper/master/LICENSE.txt",
            ],
            "org.apache.zookeeper:zookeeper-jute": [
                projectUrl: "https://github.com/apache/zookeeper",
                licenseUrl: "https://raw.githubusercontent.com/apache/zookeeper/master/LICENSE.txt",
            ],
            "org.apache.kerby:kerb-admin" : [
                projectUrl: "https://github.com/apache/directory-kerby",
                licenseUrl: "https://raw.githubusercontent.com/apache/directory-kerby/trunk/LICENSE",
            ],
            "org.apache.kerby:kerb-client" : [
                projectUrl: "https://github.com/apache/directory-kerby",
                licenseUrl: "https://raw.githubusercontent.com/apache/directory-kerby/trunk/LICENSE",
            ],
            "org.apache.kerby:kerb-common" : [
                projectUrl: "https://github.com/apache/directory-kerby",
                licenseUrl: "https://raw.githubusercontent.com/apache/directory-kerby/trunk/LICENSE",
            ],
            "org.apache.kerby:kerb-core" : [
                projectUrl: "https://github.com/apache/directory-kerby",
                licenseUrl: "https://raw.githubusercontent.com/apache/directory-kerby/trunk/LICENSE",
            ],
            "org.apache.kerby:kerb-crypto" : [
                projectUrl: "https://github.com/apache/directory-kerby",
                licenseUrl: "https://raw.githubusercontent.com/apache/directory-kerby/trunk/LICENSE",
            ],
            "org.apache.kerby:kerb-identity" : [
                projectUrl: "https://github.com/apache/directory-kerby",
                licenseUrl: "https://raw.githubusercontent.com/apache/directory-kerby/trunk/LICENSE",
            ],
            "org.apache.kerby:kerb-server" : [
                projectUrl: "https://github.com/apache/directory-kerby",
                licenseUrl: "https://raw.githubusercontent.com/apache/directory-kerby/trunk/LICENSE",
            ],
            "org.apache.kerby:kerb-simplekdc" : [
                projectUrl: "https://github.com/apache/directory-kerby",
                licenseUrl: "https://raw.githubusercontent.com/apache/directory-kerby/trunk/LICENSE",
            ],
            "org.apache.kerby:kerb-util" : [
                projectUrl: "https://github.com/apache/directory-kerby",
                licenseUrl: "https://raw.githubusercontent.com/apache/directory-kerby/trunk/LICENSE",
            ],
            "org.apache.kerby:kerby-asn1" : [
                projectUrl: "https://github.com/apache/directory-kerby",
                licenseUrl: "https://raw.githubusercontent.com/apache/directory-kerby/trunk/LICENSE",
            ],
            "org.apache.kerby:kerby-config" : [
                projectUrl: "https://github.com/apache/directory-kerby",
                licenseUrl: "https://raw.githubusercontent.com/apache/directory-kerby/trunk/LICENSE",
            ],
            "org.apache.kerby:kerby-pkix" : [
                projectUrl: "https://github.com/apache/directory-kerby",
                licenseUrl: "https://raw.githubusercontent.com/apache/directory-kerby/trunk/LICENSE",
            ],
            "org.apache.kerby:kerby-util" : [
                projectUrl: "https://github.com/apache/directory-kerby",
                licenseUrl: "https://raw.githubusercontent.com/apache/directory-kerby/trunk/LICENSE",
            ],
            "org.apache.kerby:kerby-xdr" : [
                projectUrl: "https://github.com/apache/directory-kerby",
                licenseUrl: "https://raw.githubusercontent.com/apache/directory-kerby/trunk/LICENSE",
            ],
            "org.apache.kerby:token-provider" : [
                projectUrl: "https://github.com/apache/directory-kerby",
                licenseUrl: "https://raw.githubusercontent.com/apache/directory-kerby/trunk/LICENSE",
            ],
            "org.apache.hadoop:hadoop-annotations": [
                projectUrl: "https://github.com/apache/hadoop",
                licenseUrl: "https://raw.githubusercontent.com/apache/hadoop/trunk/LICENSE.txt",
            ],
            "org.apache.hadoop:hadoop-auth": [
                projectUrl: "https://github.com/apache/hadoop",
                licenseUrl: "https://raw.githubusercontent.com/apache/hadoop/trunk/LICENSE.txt",
            ],
            "org.apache.hadoop.thirdparty:hadoop-shaded-guava": [
                projectUrl: "https://github.com/apache/hadoop-thirdparty",
                licenseUrl: "https://raw.githubusercontent.com/apache/hadoop-thirdparty/trunk/LICENSE.txt",
            ],
            "org.apache.hadoop.thirdparty:hadoop-shaded-protobuf_3_21": [
                projectUrl: "https://github.com/apache/hadoop-thirdparty",
                licenseUrl: "https://raw.githubusercontent.com/apache/hadoop-thirdparty/trunk/LICENSE.txt",
            ],
            "org.apache.curator:curator-client" : [
                projectUrl: "https://github.com/apache/curator",
                licenseUrl: "https://raw.githubusercontent.com/apache/curator/master/LICENSE",
            ],
            "org.apache.curator:curator-framework" : [
                projectUrl: "https://github.com/apache/curator",
                licenseUrl: "https://raw.githubusercontent.com/apache/curator/master/LICENSE",
            ],
            "org.apache.curator:curator-recipes" : [
                projectUrl: "https://github.com/apache/curator",
                licenseUrl: "https://raw.githubusercontent.com/apache/curator/master/LICENSE",
            ],
            "org.apache.commons:commons-compress": [
                licenseUrl: "https://raw.githubusercontent.com/apache/commons-compress/master/LICENSE.txt",
            ],
            "org.apache.commons:commons-configuration2": [
                licenseUrl: "https://raw.githubusercontent.com/apache/commons-configuration/master/LICENSE.txt",
            ],
            "org.apache.commons:commons-math3": [
                licenseUrl: "https://raw.githubusercontent.com/apache/commons-math/master/LICENSE",
            ],
            "org.apache.commons:commons-text": [
                licenseUrl: "https://raw.githubusercontent.com/apache/commons-text/master/LICENSE.txt",
            ],
            "org.apache.avro:avro": [
                licenseUrl: "https://raw.githubusercontent.com/apache/avro/main/LICENSE.txt",
            ],
            "com.sun.xml.bind:jaxb-impl": [
                licenseUrl: "https://github.com/eclipse-ee4j/jaxb-ri/blob/master/LICENSE.md",
            ],
            "commons-beanutils:commons-beanutils": [
                licenseUrl: "https://raw.githubusercontent.com/apache/commons-beanutils/master/LICENSE.txt",
            ],
            "commons-cli:commons-cli": [
                licenseUrl: "https://raw.githubusercontent.com/apache/commons-cli/master/LICENSE.txt",
            ],
            "commons-collections:commons-collections": [
                licenseUrl: "https://raw.githubusercontent.com/apache/commons-collections/master/LICENSE.txt",
            ],
            "commons-net:commons-net": [
                licenseUrl: "https://raw.githubusercontent.com/apache/commons-net/master/NOTICE.txt",
            ],
            "dnsjava:dnsjava": [
                licenseUrl: "https://raw.githubusercontent.com/dnsjava/dnsjava/master/LICENSE",
            ],
            "io.dropwizard.metrics:metrics-core": [
                projectUrl: "https://github.com/dropwizard/metrics",
                licenseUrl: "https://raw.githubusercontent.com/dropwizard/metrics/release/4.2.x/LICENSE",
            ],
            "io.netty:netty-codec": [
                projectUrl: "https://github.com/netty/netty",
                licenseUrl: "https://raw.githubusercontent.com/netty/netty/4.1/LICENSE.txt",
            ],
            "io.netty:netty-handler": [
                projectUrl: "https://github.com/netty/netty",
                licenseUrl: "https://raw.githubusercontent.com/netty/netty/4.1/LICENSE.txt",
            ],
            "io.netty:netty-resolver": [
                projectUrl: "https://github.com/netty/netty",
                licenseUrl: "https://raw.githubusercontent.com/netty/netty/4.1/LICENSE.txt",
            ],
            "io.netty:netty-transport": [
                projectUrl: "https://github.com/netty/netty",
                licenseUrl: "https://raw.githubusercontent.com/netty/netty/4.1/LICENSE.txt",
            ],
            "io.netty:netty-transport-classes-epoll": [
                projectUrl: "https://github.com/netty/netty",
                licenseUrl: "https://raw.githubusercontent.com/netty/netty/4.1/LICENSE.txt",
            ],
            "io.netty:netty-transport-native-epoll": [
                projectUrl: "https://github.com/netty/netty",
                licenseUrl: "https://raw.githubusercontent.com/netty/netty/4.1/LICENSE.txt",
            ],
            "io.netty:netty-transport-native-unix-common": [
                projectUrl: "https://github.com/netty/netty",
                licenseUrl: "https://raw.githubusercontent.com/netty/netty/4.1/LICENSE.txt",
            ],
            "com.sun.jersey:jersey-core": [
                projectUrl: "https://github.com/javaee/jersey-1.x",
                licenseUrl: "https://raw.githubusercontent.com/javaee/jersey-1.x/master/license.html",
            ],
            "com.sun.jersey:jersey-server": [
                projectUrl: "https://github.com/javaee/jersey-1.x",
                licenseUrl: "https://raw.githubusercontent.com/javaee/jersey-1.x/master/license.html",
            ],
            "com.sun.jersey:jersey-servlet": [
                projectUrl: "https://github.com/javaee/jersey-1.x",
                licenseUrl: "https://raw.githubusercontent.com/javaee/jersey-1.x/master/license.html",
            ],
            "com.jcraft:jsch": [
                licenseUrl: "http://www.jcraft.com/jsch/LICENSE.txt",
            ],
            "jakarta.activation:jakarta.activation-api": [
                projectUrl: "https://github.com/jakartaee/jaf-api",
                licenseUrl: "https://raw.githubusercontent.com/jakartaee/jaf-api/master/LICENSE.md",
            ],
            "javax.xml.bind:jaxb-api": [
                projectUrl: "https://github.com/jakartaee/jaxb-api/",
                licenseUrl: "https://raw.githubusercontent.com/jakartaee/jaxb-api/master/LICENSE.md",
            ],
            "javax.servlet:javax.servlet-api": [
                projectUrl: "https://github.com/jakartaee/servlet",
                licenseUrl: "https://raw.githubusercontent.com/jakartaee/servlet/master/LICENSE.md",
            ],
            "javax.servlet.jsp:jsp-api": [
                projectUrl: "https://github.com/jakartaee/pages",
                licenseUrl: "https://raw.githubusercontent.com/jakartaee/pages/master/LICENSE.md",
                licenseName: "Eclipse Public License - v 2.0",
            ],
            "javax.ws.rs:jsr311-api": [
                projectUrl: "https://github.com/javaee/jsr311",
                licenseUrl: "https://raw.githubusercontent.com/javaee/jsr311/master/LICENSE",
            ],
            "javax.xml.stream:stax-api": [
                projectUrl: "https://github.com/jakartaee/jaxb-api",
                licenseUrl: "https://raw.githubusercontent.com/jakartaee/jaxb-api/master/LICENSE.md",
            ],
            "org.eclipse.collections:eclipse-collections": [
                projectUrl: "https://github.com/eclipse-collections/eclipse-collections",
                licenseUrl: "https://raw.githubusercontent.com/eclipse-collections/eclipse-collections/refs/heads/master/LICENSE-EDL-1.0.txt",
            ],
            "org.eclipse.collections:eclipse-collections-api": [
                projectUrl: "https://github.com/eclipse-collections/eclipse-collections",
                licenseUrl: "https://raw.githubusercontent.com/eclipse-collections/eclipse-collections/refs/heads/master/LICENSE-EDL-1.0.txt",
            ],
            "jakarta.annotation:jakarta.annotation-api": [
                projectUrl: "https://github.com/jakartaee/common-annotations-api",
                licenseUrl: "https://raw.githubusercontent.com/jakartaee/common-annotations-api/refs/heads/master/LICENSE.md"
            ],
            "jakarta.servlet:jakarta.servlet-api": [
                projectUrl: "https://github.com/jakartaee/servlet",
                licenseUrl: "https://raw.githubusercontent.com/jakartaee/servlet/refs/heads/master/LICENSE.md"
            ],
            "com.google.api.grpc:proto-google-cloud-bigquerystorage-v1alpha": [
                projectUrl: "https://github.com/googleapis/googleapis",
                licenseUrl: "https://raw.githubusercontent.com/googleapis/googleapis/master/LICENSE"
            ],
            "com.google.api.grpc:proto-google-cloud-run-v2": [
                projectUrl: "https://github.com/googleapis/google-cloud-java",
                licenseUrl: "https://raw.githubusercontent.com/googleapis/google-cloud-java/refs/heads/main/LICENSE"
            ],
            "com.google.cloud:google-cloud-run": [
                projectUrl: "https://github.com/googleapis/google-cloud-java",
                licenseUrl: "https://raw.githubusercontent.com/googleapis/google-cloud-java/refs/heads/main/LICENSE"
            ],
            "commons-logging:commons-logging": [
                projectUrl: "https://github.com/apache/commons-logging",
                licenseUrl: "https://raw.githubusercontent.com/apache/commons-logging/refs/heads/master/LICENSE.txt"
            ],
            "io.netty:netty-codec-http": [
                projectUrl: "https://github.com/netty/netty",
                licenseUrl: "https://raw.githubusercontent.com/netty/netty/refs/heads/4.1/LICENSE.txt"
            ],
            "io.netty:netty-codec-http2": [
                projectUrl: "https://github.com/netty/netty",
                licenseUrl: "https://raw.githubusercontent.com/netty/netty/refs/heads/4.1/LICENSE.txt"
            ],
            "it.unimi.dsi:fastutil": [
                projectUrl: "http://fastutil.di.unimi.it/",
                licenseUrl: "https://raw.githubusercontent.com/vigna/fastutil/refs/heads/master/LICENSE-2.0"
            ],
            "org.apache.parquet:parquet-generator": [
                projectUrl: "https://github.com/apache/parquet-java/",
                licenseUrl: "https://raw.githubusercontent.com/apache/parquet-java/refs/heads/master/LICENSE"
            ],
            "org.reactivestreams:reactive-streams": [
                projectUrl: "https://github.com/reactive-streams/reactive-streams-jvm",
                licenseUrl: "https://raw.githubusercontent.com/reactive-streams/reactive-streams-jvm/refs/heads/master/LICENSE"
            ],
            "software.amazon.awssdk:*": [
                projectUrl: "https://github.com/aws/aws-sdk-java-v2",
                licenseUrl: "https://raw.githubusercontent.com/aws/aws-sdk-java-v2/refs/heads/master/LICENSE.txt"
            ],
            "software.amazon.eventstream:*": [
                projectUrl: "https://github.com/awslabs/aws-eventstream-java",
                licenseUrl: "https://raw.githubusercontent.com/awslabs/aws-eventstream-java/refs/heads/master/LICENSE"
            ],
            "com.google.code.javaparser:javaparser": [
                projectUrl: "https://github.com/javaparser/javaparser/",
                licenseUrl: "https://github.com/javaparser/javaparser/blob/javaparser-1.0.11/COPYING.LESSER"
            ],
            "com.googlecode.json-simple:json-simple": [
                projectUrl: "https://github.com/fangyidong/json-simple",
                licenseUrl: "https://github.com/fangyidong/json-simple/blob/master/LICENSE.txt"
            ],
            "org.apache.oozie:oozie-client": [
                projectUrl: "https://github.com/apache/oozie",
                licenseUrl: "https://github.com/apache/oozie/blob/master/LICENSE.txt"
            ],
            "org.objenesis:objenesis": [
                projectUrl: "https://github.com/easymock/objenesis",
                licenseUrl: "https://github.com/easymock/objenesis/blob/master/LICENSE.txt"
            ],
            "xerces:xercesImpl": [
                projectUrl: "https://xerces.apache.org/xerces2-j/",
                licenseUrl: "https://github.com/apache/xerces2-j/blob/Xerces-J_2_11_0/LICENSE"
            ],
            "xml-apis:xml-apis": [
                projectUrl: "https://xerces.apache.org/xml-commons/components/external/",
                // https://stackoverflow.com/questions/67599196/need-development-code-for-xml-apis-1-4-01
                licenseUrl: "https://svn.apache.org/repos/asf/xerces/xml-commons/trunk/java/external/build.xml"
            ],
            "net.java.dev.jna:jna": [
                projectUrl: "https://github.com/java-native-access/jna",
                licenseUrl: "https://github.com/java-native-access/jna/blob/master/LICENSE"
            ],
            "net.java.dev.jna:jna-platform": [
                projectUrl: "https://github.com/java-native-access/jna",
                licenseUrl: "https://github.com/java-native-access/jna/blob/master/LICENSE"
            ],
            "net.harawata:appdirs": [
                projectUrl: "https://github.com/harawata/appdirs",
                licenseUrl: "https://raw.githubusercontent.com/harawata/appdirs/refs/heads/master/LICENSE.txt"
            ],
            "com.amazonaws:aws-java-sdk-redshiftserverless": [
                licenseUrl: "https://raw.githubusercontent.com/aws/aws-sdk-java/master/LICENSE.txt"
            ],
            "com.google.api.grpc:proto-google-cloud-bigquerystorage-v1beta": [
                projectUrl: "https://github.com/googleapis/googleapis",
                licenseUrl: "https://raw.githubusercontent.com/googleapis/googleapis/master/LICENSE",
            ],
            "io.grpc:grpc-bom": [
                licenseUrl: "https://raw.githubusercontent.com/grpc/grpc-java/master/LICENSE",
            ],
            "io.netty:netty-tcnative-boringssl-static": [
                projectUrl: "https://github.com/netty/netty",
                licenseUrl: "https://raw.githubusercontent.com/netty/netty/4.1/LICENSE.txt",
            ],
            "io.netty:netty-tcnative-classes": [
                projectUrl: "https://github.com/netty/netty",
                licenseUrl: "https://raw.githubusercontent.com/netty/netty/4.1/LICENSE.txt",
            ],
            "org.apache.arrow:arrow-memory-netty-buffer-patch": [
                projectUrl: "https://github.com/apache/arrow",
                licenseUrl: "https://raw.githubusercontent.com/apache/arrow/main/LICENSE.txt",
            ],
            "org.apache.commons:commons-collections4": [
                licenseUrl: "https://raw.githubusercontent.com/apache/commons-collections/master/LICENSE.txt",
            ],
            "org.apache.hadoop.thirdparty:hadoop-shaded-protobuf_3_25": [
                projectUrl: "https://github.com/apache/hadoop-thirdparty",
                licenseUrl: "https://raw.githubusercontent.com/apache/hadoop-thirdparty/trunk/LICENSE.txt",
            ],
            "tools.profiler:async-profiler": [
                licenseUrl: "https://github.com/async-profiler/async-profiler/blob/master/LICENSE",
            ],
            "org.bouncycastle:bcprov-jdk18on": [
                projectUrl: "https://github.com/bcgit/bc-java",
                licenseUrl: "https://raw.githubusercontent.com/bcgit/bc-java/main/LICENSE.html",
            ]
        ]
    }
}
