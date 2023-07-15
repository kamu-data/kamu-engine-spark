ENGINE_VERSION = 0.19.1
SPARK_VERSION = 3.1.2
SPARK_IMAGE = bitnami/spark:$(SPARK_VERSION)
ENGINE_IMAGE_TAG = $(ENGINE_VERSION)-spark_$(SPARK_VERSION)
ENGINE_IMAGE = ghcr.io/kamu-data/engine-spark:$(ENGINE_IMAGE_TAG)


.PHONY: engine-assembly
engine-assembly:
	sbt assembly


.PHONY: adapter-assembly
adapter-assembly:
	cd adapter && \
	RUSTFLAGS="" cross build --target x86_64-unknown-linux-musl --release


.PHONY: image-build
image-build:
	docker build \
		--build-arg BASE_IMAGE=$(SPARK_IMAGE) \
		-t $(ENGINE_IMAGE) \
		-f image/Dockerfile \
		.


.PHONY: image
image: engine-assembly adapter-assembly image-build


.PHONY: image-push
image-push:
	docker push $(ENGINE_IMAGE)


.PHONY: test-data
test-data:
	mkdir -p test-data
	cd test-data && wget -O zipcodes.zip https://data.cityofnewyork.us/api/views/i8iw-xf4u/files/YObIR0MbpUVA0EpQzZSq5x55FzKGM2ejSeahdvjqR20?filename=ZIP_CODE_040114.zip