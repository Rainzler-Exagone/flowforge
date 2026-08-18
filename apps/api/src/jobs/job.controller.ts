import { Body, Controller, Get, Param, Post } from '@nestjs/common';
import { JobsService } from './job.service';

@Controller('jobs')
export class JobsController {
    constructor(private readonly jobsService: JobsService) { }

    @Post()
    create(
        @Body()
        body: {
            type: string;
            input?: unknown;
            parameters?: unknown;
        },
    ) {
        return this.jobsService.create(
            body.type,
            body.input,
            body.parameters,
        );
    }

    @Get()
    findAll() {
        return this.jobsService.findAll();
    }

    @Post(':id/retry')
    retryJobs(@Param('id') id: string) {
            return this.jobsService.retryFailedJob(id);
    }
}