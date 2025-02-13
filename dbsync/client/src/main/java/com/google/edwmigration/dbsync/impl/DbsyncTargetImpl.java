package com.google.edwmigration.dbsync.impl;

import com.google.edwmigration.dbsync.proto.Dbsync;
import com.google.edwmigration.dbsync.proto.DbsyncTargetGrpc;
import com.google.edwmigration.dbsync.proto.File;
import com.google.edwmigration.dbsync.proto.OpenRequest;
import com.google.edwmigration.dbsync.proto.OpenResponse;
import io.grpc.stub.StreamObserver;

public class DbsyncTargetImpl extends DbsyncTargetGrpc.DbsyncTargetImplBase {

  @Override
  public void doOpen(OpenRequest request, StreamObserver<OpenResponse> responseObserver) {
    OpenResponse response = OpenResponse.newBuilder()
        .build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void doFile(File request, StreamObserver<File> responseObserver) {
    super.doFile(request, responseObserver);
  }
}
