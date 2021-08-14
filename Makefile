SPARK_VERSION = 3.0.1
KAMU_VERSION = 0.0.1
ENGINE_VERSION = 0.10.5
IMAGE_REPO = kamudata
IMAGE_SPARK_UBER_TAG = $(SPARK_VERSION)_$(KAMU_VERSION)


.PHONY: image
image:
	docker build \
		--build-arg BASE_IMAGE=$(IMAGE_REPO)/spark-py-uber:$(IMAGE_SPARK_UBER_TAG) \
		-t $(IMAGE_REPO)/engine-spark:$(ENGINE_VERSION) \
		-f image/Dockerfile \
		.


.PHONY: image-push
image-push:
	docker push $(IMAGE_REPO)/engine-spark:$(ENGINE_VERSION)


.PHONY: test-data
test-data:
	mkdir -p test-data
	cd test-data && wget -O zipcodes.zip https://data.cityofnewyork.us/api/views/i8iw-xf4u/files/YObIR0MbpUVA0EpQzZSq5x55FzKGM2ejSeahdvjqR20?filename=ZIP_CODE_040114.zip