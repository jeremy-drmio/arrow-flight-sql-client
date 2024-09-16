plugins {
    id("java")
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {

    //runtimeOnly(fileTree("libs") { include("*.jar") })
    runtimeOnly("org.apache.arrow:flight-sql-jdbc-driver:17.0.0")

    implementation("com.google.code.gson:gson:2.10")

    testImplementation(platform("org.junit:junit-bom:5.10.3"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.test {
    useJUnitPlatform()
}