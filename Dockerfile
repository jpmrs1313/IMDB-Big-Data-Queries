FROM bde2020/hadoop-base
COPY target/ggcd_project-1.0-SNAPSHOT.jar /
CMD [ "hadoop", "jar", "/ggcd_project-1.0-SNAPSHOT.jar", "Main.Main" ]