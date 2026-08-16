import { Test, TestingModule } from '@nestjs/testing';
import { ImageWorkerController } from './image-worker.controller';
import { ImageWorkerService } from './image-worker.service';

describe('ImageWorkerController', () => {
  let imageWorkerController: ImageWorkerController;

  beforeEach(async () => {
    const app: TestingModule = await Test.createTestingModule({
      controllers: [ImageWorkerController],
      providers: [ImageWorkerService],
    }).compile();

    imageWorkerController = app.get<ImageWorkerController>(ImageWorkerController);
  });

  describe('root', () => {
    it('should return "Hello World!"', () => {
      expect(imageWorkerController.getHello()).toBe('Hello World!');
    });
  });
});
