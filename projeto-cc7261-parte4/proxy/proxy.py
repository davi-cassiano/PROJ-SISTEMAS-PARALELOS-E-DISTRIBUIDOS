import zmq

context = zmq.Context()

# os servidores publicam aqui
sub = context.socket(zmq.XSUB)
sub.bind("tcp://*:5557")

# os bots se inscrevem aqui
pub = context.socket(zmq.XPUB)
pub.bind("tcp://*:5558")

print("proxy pub/sub rodando (XSUB 5557, XPUB 5558)")

zmq.proxy(pub, sub)

pub.close()
sub.close()
context.close()
