package main

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/credentials"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"

	pb "github.com/tech-with-moss/go-usermgmt-grpc/usermgmt"
	"google.golang.org/grpc"
)

const (
	port = 50051
)

type ServerGRPC struct {
	logger      *logrus.Logger
	server      *grpc.Server
	port        int
	certificate string
	key         string
	pb.UnimplementedUserManagementServer
}

type ServerGRPCConfig struct {
	Certificate string
	Key         string
	Port        int
}

func (s *ServerGRPC) CreateNewUser(ctx context.Context, in *pb.NewUser) (*pb.User, error) {
	log.Printf("Received: %v, at age %d", in.GetName(), in.GetAge())
	var user_id int32 = int32(rand.Intn(100))
	return &pb.User{Name: in.GetName(), Age: in.GetAge(), Id: user_id}, nil
}

func NewServerGRPC(cfg ServerGRPCConfig) (s ServerGRPC, err error) {
	s.logger = logrus.StandardLogger()

	if cfg.Port == 0 {
		err = errors.Errorf("Port must be specified")
		return
	}

	s.port = cfg.Port
	s.certificate = cfg.Certificate
	s.key = cfg.Key

	return
}

func (s *ServerGRPC) Listen() (err error) {
	var (
		listener  net.Listener
		grpcOpts  = []grpc.ServerOption{}
		grpcCreds credentials.TransportCredentials
	)

	listener, err = net.Listen("tcp", ":"+strconv.Itoa(s.port))
	if err != nil {
		err = errors.Wrapf(err,
			"failed to listen on port %d",
			s.port)
		return
	}

	if s.certificate != "" && s.key != "" {
		grpcCreds, err = credentials.NewServerTLSFromFile(
			s.certificate, s.key)
		if err != nil {
			err = errors.Wrapf(err,
				"failed to create tls grpc server using cert %s and key %s",
				s.certificate, s.key)
			return
		}

		grpcOpts = append(grpcOpts, grpc.Creds(grpcCreds))
	}

	s.server = grpc.NewServer(grpcOpts...)
	pb.RegisterUserManagementServer(s.server, s)

	err = s.server.Serve(listener)
	if err != nil {
		err = errors.Wrapf(err, "errored listening for grpc connections")
		return
	}

	return
}

func (s *ServerGRPC) Upload(stream pb.UserManagement_UploadServer) (err error) {
	var file []byte
	for {
		r, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}

			err = errors.Wrapf(err,
				"failed unexpectedly while reading chunks from stream")
			return fmt.Errorf("zhep : %v", err)
		}
		file = append(file, r.GetContent()...)
	}

	s.logger.Println("upload received")

	err = os.WriteFile("received", file, 0644)
	if err != nil {
		s.logger.Println("cant write file")
	}
	err = stream.SendAndClose(&pb.UploadStatus{
		Message: "Upload received with success",
		Code:    pb.UploadStatusCode_Ok,
	})
	if err != nil {
		err = errors.Wrapf(err,
			"failed to send status code")
		return
	}

	return
}

func (s *ServerGRPC) Close() {
	if s.server != nil {
		s.server.Stop()
	}

	return
}

func must(err error) {
	if err == nil {
		return
	}

	fmt.Printf("ERROR: %+v\n", err)
	os.Exit(1)
}

func main() {
	grpcServer, err := NewServerGRPC(ServerGRPCConfig{
		Port: port,
		//Certificate: certificate,
		//Key:         key,
	})
	must(err)
	server := &grpcServer
	err = server.Listen()
	must(err)
	defer server.Close()

	return
	//lis, err := net.Listen("tcp", port)
	//if err != nil {
	//	log.Fatalf("failed to listen: %v", err)
	//}
	//s := grpc.NewServer()
	//pb.RegisterUserManagementServer(s, &ServerGRPC{})
	//log.Printf("server listening at %v", lis.Addr())
	//if err := s.Serve(lis); err != nil {
	//	log.Fatalf("failed to serve: %v", err)
	//}
}
