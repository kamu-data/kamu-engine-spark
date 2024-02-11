ENGINE_VERSION = 0.23.0
SPARK_VERSION = 3.5.0
SPARK_IMAGE = bitnami/spark:$(SPARK_VERSION)
ENGINE_IMAGE_TAG = $(ENGINE_VERSION)-spark_$(SPARK_VERSION)
ENGINE_IMAGE = ghcr.io/kamu-data/engine-spark:$(ENGINE_IMAGE_TAG)


.PHONY: engine-assembly
engine-assembly:
	sbt assembly


.PHONY: adapter-assembly
adapter-assembly:
	mkdir -p image/tmp/linux/amd64
	cd adapter && RUSTFLAGS="" cross build --target x86_64-unknown-linux-musl --release
	cp adapter/target/x86_64-unknown-linux-musl/release/kamu-engine-spark-adapter image/tmp/linux/amd64/adapter

.PHONY: adapter-assembly-multi-arch
adapter-assembly-multi-arch:
	rm -rf image/tmp
	mkdir -p image/tmp/linux/amd64 image/tmp/linux/arm64
	cd adapter && \
	RUSTFLAGS="" cross build --target x86_64-unknown-linux-musl --release && \
	RUSTFLAGS="" cross build --target aarch64-unknown-linux-musl --release
	cp adapter/target/x86_64-unknown-linux-musl/release/kamu-engine-spark-adapter image/tmp/linux/amd64/adapter
	cp adapter/target/aarch64-unknown-linux-musl/release/kamu-engine-spark-adapter image/tmp/linux/arm64/adapter


.PHONY: image-build
image-build:
	docker build \
		--build-arg BASE_IMAGE=$(SPARK_IMAGE) \
		-t $(ENGINE_IMAGE) \
		-f image/Dockerfile \
		.

.PHONY: image-build-multi-arch
image-build-multi-arch:
	docker buildx build \
		--push \
		--platform linux/amd64,linux/arm64 \
		--build-arg BASE_IMAGE=$(SPARK_IMAGE) \
		-t $(ENGINE_IMAGE) \
		-f image/Dockerfile \
		.


.PHONY: image
image: engine-assembly adapter-assembly image-build

.PHONY: image-multi-arch
image-multi-arch: engine-assembly adapter-assembly-multi-arch image-build-multi-arch


.PHONY: image-push
image-push:
	docker push $(ENGINE_IMAGE)


.PHONY: test-data
test-data:
	mkdir -p test-data
	cd test-data && wget -O zipcodes.zip https://data.cityofnewyork.us/api/views/i8iw-xf4u/files/YObIR0MbpUVA0EpQzZSq5x55FzKGM2ejSeahdvjqR20?filename=ZIP_CODE_040114.zip