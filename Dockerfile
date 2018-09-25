FROM busybox

ADD ./benchio /bin/

CMD ["benchio"]
