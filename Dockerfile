FROM maven:3.6.3-openjdk-15

RUN yum -y install libX11-devel.x86_64
RUN yum -y install libXext.x86_64
RUN yum -y install libXrender.x86_64
RUN yum -y install libXtst.x86_64
COPY . /usr/src/myapp
WORKDIR /usr/src/myapp
CMD ["java", "-cp", ".:./lib/*", "Client"]