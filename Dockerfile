FROM gmag11/metatrader5_vnc:latest

ENV CUSTOM_USER=weltrade
ENV PASSWORD=weltrade2024

EXPOSE 3000 8001 1234

CMD ["/bin/bash", "-c", "service supervisor restart && tail -f /dev/null"]
