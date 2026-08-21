# Tell Terraform to use AWS
provider "aws" {
  region = "ap-south-1"  # Mumbai region (free tier)
}

# Get the latest Amazon Linux 2 AMI
data "aws_ssm_parameter" "amazon_linux_2" {
  name = "/aws/service/ami-amazon-linux-latest/amzn2-ami-hvm-x86_64-gp2"
}

# Create a security group (firewall)
resource "aws_security_group" "crypto_sg" {
  name        = "crypto-platform-sg-2026"
  description = "Allow SSH, HTTP, and FastAPI ports"

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]  # SSH from anywhere (for now)
  }

  ingress {
    from_port   = 8000
    to_port     = 8501
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]  # FastAPI + Streamlit
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# Create an EC2 instance (t2.micro = free tier)
resource "aws_instance" "crypto_server" {
  ami           = data.aws_ssm_parameter.amazon_linux_2.value
  instance_type = "t3.micro"
  key_name = "crypto-key-final"

  security_groups = [aws_security_group.crypto_sg.name]

  user_data = <<-EOF
    #!/bin/bash
    sudo yum update -y
    sudo yum install -y docker git
    sudo systemctl start docker
    sudo systemctl enable docker
    sudo usermod -a -G docker ec2-user

    # Install Docker Compose (Plugin v2)
    sudo yum install -y docker-compose-plugin

    # Clone the repo and start the apps
    git clone https://github.com/rIS-spec/real-time-crypto-data-platform.git /home/ec2-user/real-time-crypto-data-platform
    cd /home/ec2-user/real-time-crypto-data-platform
    sudo docker compose up -d
  EOF

  tags = {
    Name = "Crypto-Platform-Server"
  }
}

output "public_ip" {
  value = aws_instance.crypto_server.public_ip
}