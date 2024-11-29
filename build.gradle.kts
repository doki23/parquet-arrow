import org.gradle.launcher.daemon.protocol.Build

plugins {
    `java-library`
}

repositories {
    mavenCentral()
}

dependencies {
    api("org.apache.parquet:parquet-column:1.14.1")
    api("org.apache.parquet:parquet-hadoop:1.14.1")
    api("org.apache.parquet:parquet-arrow:1.14.1")
    api("org.apache.arrow:arrow-vector:17.0.0")
    api("org.apache.arrow:arrow-memory-netty:17.0.0")
    implementation(libs.guava)
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.hadoop.client)
    testImplementation(libs.hadoop.common) {
        exclude(group = "org.slf4j", module = "slf4j-api")
        exclude(group = "org.slf4j", module = "slf4j-log4j12")
        exclude(group = "org.slf4j", module = "slf4j-reload4j")
        exclude(group = "ch.qos.logback", module = "logback-core")
        exclude(group = "ch.qos.logback", module = "logback-classic")
    }
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
}

tasks.compileTestJava {
    doFirst {
        val initResult = exec {
            commandLine("git", "submodule", "update", "--init")
        }
        if (initResult.exitValue != 0) {
            throw GradleException("Failed to initialize git submodules")
        }
    }
}

tasks.named<Test>("test") {
    useJUnitPlatform()
    jvmArgs("--add-opens", "java.base/java.nio=ALL-UNNAMED")
}